import json
import shutil
from pathlib import Path

import pandas as pd

from app.core.config import settings
from app.core.logging import get_logger
from app.domain.models import RawMarketEvent

logger = get_logger(__name__)


class FileExtractor:
    """
    Extracts market events from CSV or JSON files.

    File lifecycle:
    1. Reads file from input_dir
    2. On success: moves to archive_dir
    3. On failure: moves to error_dir
    """

    def __init__(
        self,
        input_dir: str = settings.FILE_INPUT_DIR,
        archive_dir: str = settings.FILE_ARCHIVE_DIR,
        error_dir: str = settings.FILE_ERROR_DIR,
    ) -> None:
        self.input_dir = Path(input_dir)
        self.archive_dir = Path(archive_dir)
        self.error_dir = Path(error_dir)

        for d in [self.input_dir, self.archive_dir, self.error_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def get_pending_files(self) -> list[Path]:
        """Returns all CSV and JSON files in the input directory."""
        files = list(self.input_dir.glob("*.csv")) + list(self.input_dir.glob("*.json"))
        return sorted(files)

    def read_csv(self, file_path: Path) -> list[dict]:
        """Reads a CSV file and returns a list of row dicts."""
        df = pd.read_csv(
            file_path,
            dtype=str,  # read everything as string first — cleaner handles types
            keep_default_na=False,
        )
        return df.to_dict(orient="records")

    def read_json(self, file_path: Path) -> list[dict]:
        """Reads a JSON file (array or newline-delimited JSON)."""
        content = file_path.read_text(encoding="utf-8")
        try:
            data = json.loads(content)
            return data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            # Try newline-delimited JSON (NDJSON)
            return [json.loads(line) for line in content.strip().split("\n") if line.strip()]

    def extract_file(self, file_path: Path) -> tuple[list[RawMarketEvent], int]:
        """
        Reads a file and returns (RawMarketEvent list, skipped_count).
        Archives the file on success, moves to error dir on failure.
        """
        logger.info("Reading file", path=str(file_path), format=file_path.suffix)

        try:
            raw_rows = (
                self.read_csv(file_path)
                if file_path.suffix.lower() == ".csv"
                else self.read_json(file_path)
            )

            events: list[RawMarketEvent] = []
            skipped = 0

            for i, row in enumerate(raw_rows):
                try:
                    event = RawMarketEvent(
                        symbol=str(row.get("symbol", "")),
                        price=row.get("price"),
                        volume=row.get("volume"),
                        currency=str(row.get("currency", "")),
                        market=str(row.get("market", "")),
                        timestamp=str(row.get("timestamp", "")),
                        source=file_path.stem,
                        raw_payload=row,
                    )
                    events.append(event)
                except Exception as e:
                    logger.warning("Skipping malformed row", row_index=i, error=str(e))
                    skipped += 1

            self._archive(file_path)
            logger.info(
                "File extraction complete",
                file=file_path.name,
                extracted=len(events),
                skipped=skipped,
            )
            return events, skipped

        except Exception as e:
            self._move_to_errors(file_path)
            logger.error("File extraction failed", file=file_path.name, error=str(e))
            raise

    def _archive(self, file_path: Path) -> None:
        dest = self.archive_dir / file_path.name
        shutil.move(str(file_path), str(dest))

    def _move_to_errors(self, file_path: Path) -> None:
        dest = self.error_dir / file_path.name
        shutil.move(str(file_path), str(dest))
