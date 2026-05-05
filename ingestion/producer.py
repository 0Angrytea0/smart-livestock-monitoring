import json
import math
import time
import socket
import asyncio
import heapq
import csv
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Callable, Dict, Any, List, Optional, Tuple, DefaultDict
from collections import defaultdict

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


@dataclass
class SensorConfig:
    name: str
    topic: str
    data_dir: Path
    build_event: Callable[[Dict[str, str], str], Dict[str, Any]]


KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
SIMULATION_TICK_SECONDS = 1.0
TIME_ACCELERATION = 10.0


def unix_to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def build_cbt_event(row, cow_id):
    return {
        "time": unix_to_iso(float(row["timestamp"])),
        "type": "cow",
        "id": cow_id,
        "sensor": "cbt",
        "metrics": {
            "temperature": float(row["temperature_C"])
        }
    }


def build_ankle_event(row, cow_id):
    return {
        "time": unix_to_iso(float(row["timestamp"])),
        "type": "cow",
        "id": cow_id,
        "sensor": "ankle",
        "metrics": {
            "lying": int(float(row["lying"]))
        }
    }


def build_immu_event(row, tag_id):
    return {
        "time": unix_to_iso(float(row["timestamp"])),
        "type": "tag",
        "id": tag_id,
        "sensor": "immu",
        "metrics": {
            "accel_x_mps2": float(row["accel_x_mps2"]),
            "accel_y_mps2": float(row["accel_y_mps2"]),
            "accel_z_mps2": float(row["accel_z_mps2"]),
            "mag_x_uT": float(row["mag_x_uT"]),
            "mag_y_uT": float(row["mag_y_uT"]),
            "mag_z_uT": float(row["mag_z_uT"])
        }
    }


def build_pressure_event(row, tag_id):
    return {
        "time": unix_to_iso(float(row["timestamp"])),
        "type": "tag",
        "id": tag_id,
        "sensor": "pressure",
        "metrics": {
            "pressure_Pa": float(row["pressure_Pa"]),
            "elevation_m": float(row["elevation_m"])
        }
    }


def build_uwb_event(row, tag_id):
    return {
        "time": unix_to_iso(float(row["timestamp"])),
        "type": "tag",
        "id": tag_id,
        "sensor": "uwb",
        "metrics": {
            "coord_x_cm": float(row["coord_x_cm"]),
            "coord_y_cm": float(row["coord_y_cm"]),
            "coord_z_cm": float(row["coord_z_cm"])
        }
    }


def build_milk_event(row, cow_id):
    return {
        "time": unix_to_iso(float(row["timestamp"])),
        "type": "cow",
        "id": cow_id,
        "sensor": "milk",
        "metrics": {
            "milk_weight_kg": float(row["milk_weight_kg"]),
            "DIM": float(row["DIM"])
        }
    }


def build_thi_event(row, sensor_id):
    return {
        "time": unix_to_iso(float(row["timestamp"])),
        "type": "environment",
        "id": sensor_id,
        "sensor": "thi",
        "metrics": {
            "temperature_C": float(row["temperature_C"]),
            "humidity_per": float(row["humidity_per"]),
            "THI": float(row["THI"])
        }
    }


def wait_for_kafka(bootstrap_server: str, timeout: int = 60):
    start = time.time()
    host, port = bootstrap_server.split(":")
    port = int(port)

    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                try:
                    test_producer = KafkaProducer(bootstrap_servers=bootstrap_server)
                    test_producer.close()
                    print(f"Kafka готова на {bootstrap_server}")
                    return
                except NoBrokersAvailable:
                    pass
        except (ConnectionRefusedError, OSError):
            pass

        if time.time() - start > timeout:
            raise TimeoutError(f"Kafka не доступна на {bootstrap_server} после {timeout} сек")

        print("Kafka ещё не доступна, жду 1 сек...")
        time.sleep(1)


def group_files_by_entity(sensor: SensorConfig) -> Dict[str, List[Path]]:
    groups: DefaultDict[str, List[Path]] = defaultdict(list)

    for f in sorted(sensor.data_dir.rglob("*.csv")):
        if f.stem == "average":
            continue
        entity_id = f.stem.split("_")[0]
        groups[entity_id].append(f)

    return dict(groups)


class EntityStream:
    def __init__(self, sensor: SensorConfig, entity_id: str, files: List[Path]):
        self.sensor = sensor
        self.entity_id = entity_id
        self.files = sorted(files)

        self.file_index = 0
        self.initialized = False

        self.current_file_path: Optional[Path] = None
        self.current_file_obj = None
        self.current_reader: Optional[csv.DictReader] = None
        self.peeked_row: Optional[Dict[str, str]] = None
        self.peeked_ts: Optional[float] = None

    def _open_next_file(self) -> bool:
        self._close_current_file()

        while self.file_index < len(self.files):
            csv_file = self.files[self.file_index]
            self.file_index += 1

            print(f"[{self.sensor.name}] Открываю файл: {csv_file}")

            f = None
            try:
                f = open(csv_file, "r", encoding="utf-8-sig", newline="")
                reader = csv.DictReader(f)

                if reader.fieldnames is not None:
                    reader.fieldnames = [name.strip() if name is not None else name for name in reader.fieldnames]

                first_row = next(reader, None)

                if first_row is None:
                    print(f"[{self.sensor.name}] Пустой файл: {csv_file}")
                    f.close()
                    continue

                first_row = {
                    (k.strip() if k is not None else k): v
                    for k, v in first_row.items()
                }

                if "timestamp" not in first_row:
                    raise KeyError(f"Колонки файла {csv_file}: {list(first_row.keys())}")

                ts = float(first_row["timestamp"])

                self.current_file_path = csv_file
                self.current_file_obj = f
                self.current_reader = reader
                self.peeked_row = first_row
                self.peeked_ts = ts
                return True

            except Exception as e:
                print(f"[{self.sensor.name}] Ошибка чтения файла {csv_file}: {e}")
                if f is not None:
                    try:
                        f.close()
                    except Exception:
                        pass
                continue

        return False

    def _close_current_file(self):
        if self.current_file_obj is not None:
            try:
                self.current_file_obj.close()
            except Exception:
                pass

        self.current_file_path = None
        self.current_file_obj = None
        self.current_reader = None
        self.peeked_row = None
        self.peeked_ts = None

    def _ensure_initialized(self):
        if not self.initialized:
            self.initialized = True
            self._open_next_file()

    def peek_timestamp(self) -> Optional[float]:
        self._ensure_initialized()
        return self.peeked_ts

    def pop_event(self) -> Optional[Tuple[float, Dict[str, Any]]]:
        self._ensure_initialized()

        if self.peeked_row is None or self.peeked_ts is None:
            return None

        row = self.peeked_row
        ts = self.peeked_ts
        event = self.sensor.build_event(row, self.entity_id)

        next_row = None
        if self.current_reader is not None:
            next_row = next(self.current_reader, None)

        if next_row is not None:
            self.peeked_row = next_row
            self.peeked_ts = float(next_row["timestamp"])
        else:
            if not self._open_next_file():
                self._close_current_file()

        return ts, event

    def close(self):
        self._close_current_file()


def build_streams(sensor_configs: List[SensorConfig]) -> List[EntityStream]:
    streams: List[EntityStream] = []

    for sensor in sensor_configs:
        grouped = group_files_by_entity(sensor)
        print(f"[{sensor.name}] Найдено потоков: {len(grouped)}")

        for entity_id, files in sorted(grouped.items()):
            print(f"[{sensor.name}] Старт потока {entity_id}, файлов: {len(files)}")
            streams.append(EntityStream(sensor, entity_id, files))

    return streams


async def run_tick_simulation(streams: List[EntityStream], producer: KafkaProducer):
    if not streams:
        print("Нет доступных потоков с данными")
        return

    print("Инициализирую первые timestamp потоков...")

    heap: List[Tuple[float, int, EntityStream]] = []
    seq = 0

    for stream in streams:
        ts = stream.peek_timestamp()
        if ts is not None:
            heapq.heappush(heap, (ts, seq, stream))
            seq += 1

    if not heap:
        print("После инициализации не найдено ни одного события")
        return

    global_start_ts = min(item[0] for item in heap)
    tick_start = math.floor(global_start_ts)

    print(f"Глобальный старт симуляции: {global_start_ts} ({unix_to_iso(global_start_ts)})")
    print(f"Первый тик: {tick_start} ({unix_to_iso(tick_start)})")
    print(f"Размер тика: {SIMULATION_TICK_SECONDS} сек")
    print(f"TIME_ACCELERATION: {TIME_ACCELERATION}")

    sim_start_monotonic = time.monotonic()
    tick_number = 0
    total_sent = 0

    while heap:
        tick_end = tick_start + SIMULATION_TICK_SECONDS
        tick_events_count = 0

        while heap and heap[0][0] < tick_end:
            _, _, stream = heapq.heappop(heap)

            while True:
                next_ts = stream.peek_timestamp()
                if next_ts is None or next_ts >= tick_end:
                    break

                popped = stream.pop_event()
                if popped is None:
                    break

                _, event = popped

                producer.send(
                    stream.sensor.topic,
                    key=stream.entity_id,
                    value=event
                )

                tick_events_count += 1
                total_sent += 1

            next_ts_after_send = stream.peek_timestamp()
            if next_ts_after_send is not None:
                heapq.heappush(heap, (next_ts_after_send, seq, stream))
                seq += 1

        if tick_events_count > 0:
            producer.flush()
            print(
                f"[TICK {tick_number}] "
                f"{unix_to_iso(tick_start)} .. {unix_to_iso(tick_end)} | "
                f"sent={tick_events_count} | total={total_sent}"
            )

        tick_number += 1
        tick_start = tick_end

        dataset_elapsed = tick_start - global_start_ts
        target_monotonic = sim_start_monotonic + (dataset_elapsed / TIME_ACCELERATION)
        sleep_time = target_monotonic - time.monotonic()

        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

    producer.flush()
    print(f"Симуляция завершена. Всего отправлено сообщений: {total_sent}")


async def main():
    wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks=1,
        linger_ms=50,
        batch_size=32768,
        compression_type=None,
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=30000,
        max_block_ms=30000,
    )

    cbt_sensor = SensorConfig(
        name="cbt",
        topic="mmcows.cbt",
        data_dir=Path("data/sensor_data/main_data/cbt"),
        build_event=build_cbt_event
    )

    ankle_sensor = SensorConfig(
        name="ankle",
        topic="mmcows.ankle",
        data_dir=Path("data/sensor_data/main_data/ankle"),
        build_event=build_ankle_event
    )

    immu_sensor = SensorConfig(
        name="immu",
        topic="mmcows.immu",
        data_dir=Path("data/sensor_data/main_data/immu"),
        build_event=build_immu_event
    )

    milk_sensor = SensorConfig(
        name="milk",
        topic="mmcows.milk",
        data_dir=Path("data/sensor_data/main_data/milk"),
        build_event=build_milk_event
    )

    pressure_sensor = SensorConfig(
        name="pressure",
        topic="mmcows.pressure",
        data_dir=Path("data/sensor_data/main_data/pressure"),
        build_event=build_pressure_event
    )

    uwb_sensor = SensorConfig(
        name="uwb",
        topic="mmcows.uwb",
        data_dir=Path("data/sensor_data/main_data/uwb"),
        build_event=build_uwb_event
    )

    thi_sensor = SensorConfig(
        name="thi",
        topic="mmcows.thi",
        data_dir=Path("data/sensor_data/main_data/thi"),
        build_event=build_thi_event
    )

    sensor_configs = [
        cbt_sensor,
        ankle_sensor,
        immu_sensor,
        milk_sensor,
        pressure_sensor,
        uwb_sensor,
        thi_sensor
    ]

    streams: List[EntityStream] = []

    try:
        streams = build_streams(sensor_configs)
        print(f"Всего потоков создано: {len(streams)}")
        await run_tick_simulation(streams, producer)
    finally:
        for stream in streams:
            stream.close()
        producer.flush()
        producer.close()

    print("All sensors finished")


if __name__ == "__main__":
    asyncio.run(main())