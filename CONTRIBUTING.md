# Contributing to FeatherStore

Thank you for your interest in contributing to FeatherStore! This document provides guidelines and instructions for contributing.

## Code of Conduct

Please be respectful and considerate of others when contributing to this project. We expect all contributors to adhere to professional standards of communication and behavior.

## Getting Started

1. **Fork the repository**
2. **Clone your fork locally**
   ```bash
   git clone https://github.com/TFMV/featherstore.git
   cd featherstore
   ```
3. **Set up development environment**
   ```bash
   make setup-dev
   ```
4. **Create a branch for your work**
   ```bash
   git checkout -b your-feature-name
   ```

## Development Process

### Making Changes

1. Make your changes to the codebase
2. Write or update tests as necessary
3. Run tests to ensure they pass:
   ```bash
   make test
   ```
4. Format your code:
   ```bash
   make fmt
   ```
5. Run linters:
   ```bash
   make lint
   ```

### Building and Running Locally

To build the application:
```bash
make build
```

To run the application locally:
```bash
make run
```

### Docker-based Development

To build a Docker image:
```bash
make docker-build
```

To run the application in Docker:
```bash
make docker-run
```

Or use Docker Compose:
```bash
docker-compose up
```

## Pull Request Process

1. **Update documentation**: If your changes include new features or modified functionality, update the relevant documentation.

2. **Update tests**: Ensure your changes are covered by tests. Add new tests if needed.

3. **Submit your pull request**: Push your changes to your fork and submit a pull request against the main repository.
   - Describe what your changes do
   - Reference any related issues
   - Include screenshots or output examples if relevant

4. **Code review**: Maintainers will review your PR and may request changes. Be responsive to feedback.

5. **Merge**: Once approved, your PR will be merged by a maintainer.

## Architecture Guidelines

### Code Organization

- Follow the established package structure
- Keep dependencies between packages clean and explicit
- Use interfaces appropriately for decoupling

### Coding Style

- Follow idiomatic Go conventions (use `gofmt`, `golint`)
- Write clear, concise comments
- Use descriptive variable and function names
- Keep functions small and focused

### Testing

- Write unit tests for all new code
- Aim for high test coverage
- Use table-driven tests where appropriate
- Mock external dependencies

## Reporting Issues

When reporting issues, please include:
- A clear description of the problem
- Steps to reproduce
- Expected vs. actual behavior
- Version information (Go version, OS, etc.)
- Logs or error output if available

## Feature Requests

Feature requests are welcome! Please include:
- A clear description of the proposed feature
- Use cases that illustrate why the feature would be valuable
- Any relevant background information or references

## License

By contributing to FeatherStore, you agree that your contributions will be licensed under the project's Apache 2.0 License. 