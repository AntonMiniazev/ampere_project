"""Start the accepted Raw change journal to Bronze state application."""

from iceberg_bronze.journal_apply import run_cli


if __name__ == "__main__":
    run_cli()
