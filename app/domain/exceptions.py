class ETLException(Exception):
    """Base ETL pipeline exception."""

    pass


class ExtractionError(ETLException):
    def __init__(self, source: str, message: str):
        super().__init__(f"Extraction failed from {source}: {message}")


class ValidationError(ETLException):
    def __init__(self, record_id: str, field: str, message: str):
        super().__init__(f"Validation failed for record {record_id}, field={field}: {message}")


class TransformationError(ETLException):
    def __init__(self, stage: str, message: str):
        super().__init__(f"Transformation failed at stage '{stage}': {message}")


class LoadError(ETLException):
    def __init__(self, table: str, message: str):
        super().__init__(f"Load failed to table '{table}': {message}")


class PipelineNotFoundError(ETLException):
    def __init__(self, run_id: str):
        super().__init__(f"Pipeline run not found: {run_id}")
