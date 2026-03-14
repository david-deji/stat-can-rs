# Contributing to stat-can-rs

Thank you for your interest in contributing to `stat-can-rs`! This guide will help you get started with the development process.

## Environment Setup

### Prerequisites

- **Rust**: [Install Rust](https://www.rust-lang.org/tools/install) (Edition 2021)
- **Python**: Version 3.8 or higher
- **Maturin**: For building the Python bindings (`pip install maturin`)
- **Pre-commit**: For automated linting (`pip install pre-commit`)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/poche450/stat-can-rs.git
   cd stat-can-rs
   ```

2. Install pre-commit hooks:
   ```bash
   pre-commit install
   ```

3. Build the project:
   ```bash
   make build
   ```

## Development Workflow

We use a `Makefile` to simplify common development tasks.

### Core Commands

- **Format code**: `make fmt`
- **Lint code**: `make lint`
- **Run tests**: `make test`
- **Build project**: `make build`

## Coding Standards

### Rust
- Follow standard Rust naming conventions.
- Run `cargo clippy` and ensure no warnings are present.
- Use `rustfmt` to format your code.

### Python
- We use `ruff` for linting and formatting Python code.
- Ensure all Python bindings are correctly exported and documented.

## Pull Request Process

1. Create a new branch for your feature or bugfix.
2. Implement your changes and add tests if applicable.
3. Ensure all tests pass and linting is clean (`make test lint`).
4. Submit a Pull Request to the `master` branch.
5. Provide a clear description of the changes and any relevant issue numbers.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
