from orchestrator.config import MacSettings


def main() -> None:
    settings = MacSettings()
    print(f"JAV Subtitle Orchestrator API: {settings.host}:{settings.port}")


if __name__ == "__main__":
    main()
