# Contributing to Heartbeat Monitoring Pipeline

Thank you for considering contributing to this project!

## Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/Emmanuel-kabu/Heart_beat_monitoring_pipeline.git
   cd Heart_beat_monitoring_pipeline
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   ```

3. Install development dependencies:
   ```bash
   pip install -r requirements.txt
   pip install -e ".[dev,dashboard]"
   ```

4. Copy environment file:
   ```bash
   cp .env.example .env
   ```

## Code Style

- Follow PEP 8 guidelines
- Use `black` for formatting (line length: 100)
- Use `isort` for import sorting
- Run `flake8` for linting
- Add type hints to all function signatures

## Testing

- Write tests in `tests/` directory
- Use `pytest` as the test framework
- Aim for >80% code coverage
- Run tests before submitting PRs:
  ```bash
  make test
  make test-coverage
  ```

## Commit Messages

Follow conventional commits:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation
- `test:` Tests
- `build:` Build system
- `chore:` Maintenance
- `infra:` Infrastructure

## Pull Requests

1. Create a feature branch
2. Write tests for new functionality
3. Ensure all tests pass
4. Update documentation if needed
5. Submit PR with clear description
