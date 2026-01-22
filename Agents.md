# Python Development Guidelines

## Development Philosophy

- **Simplicity**: Write simple, straightforward code
- **Readability**: Make code easy to understand
- **Performance**: Consider performance without sacrificing readability
- **Maintainability**: Write code that's easy to update
- **Testability**: Ensure code is testable and test frequently
- **Reusability**: Create reusable components and functions
- **Less Code = Less Debt**: Minimize code footprint

## Coding Best Practices

- **Early Returns**: Use to avoid nested conditions
- **Descriptive Names**: Use clear variable/function names (prefix handlers with `handle_`)
- **Constants Over Functions**: Use constants where possible
- **DRY Code**: Don't repeat yourself
- **Functional Style**: Prefer functional, immutable approaches when not verbose
- **Minimal Changes**: Only modify code related to the task at hand
- **Function Ordering**: Define composing functions before their components
- **TODO Comments**: Mark issues in existing code with `TODO:` prefix
- **Simplicity**: Prioritize simplicity and readability over clever solutions
- **Build Iteratively**: Start with minimal functionality and verify it works before adding complexity
- **Run Tests**: Test code frequently with realistic inputs and validate outputs
- **Build Test Environments**: Create testing environments for components that are difficult to validate directly
- **Clean Logic**: Keep core logic clean and push implementation details to the edges
- **File Organization**: Balance file organization with simplicity—use an appropriate number of files for the project scale
- **Imports**: Avoid importing the entire module, import only what is needed and make it top-level imports

## OOP & SOLID Principles

- **Single Responsibility**: Each class/module should have one reason to change
- **Open/Closed**: Open for extension, closed for modification
- **Liskov Substitution**: Subtypes must be substitutable for their base types
- **Interface Segregation**: Prefer small, focused interfaces over large ones
- **Dependency Inversion**: Depend on abstractions, not concretions

## Composition & Coupling

- Favor composition over deep inheritance hierarchies
- Use mixins sparingly; prefer delegation
- Inherit for "is-a", compose for "has-a"
- **Low coupling**: Minimize dependencies between modules
- **High cohesion**: Related functionality should live together
- Avoid circular imports; refactor shared code into a common module

## Design Patterns

- Use **Strategy** pattern to swap algorithms at runtime
- Use **Factory** pattern to decouple object creation
- Use **Dependency Injection** to make classes testable and flexible

## Python-Specific

- Use `abc.ABC` and `@abstractmethod` for explicit interfaces
- Use `typing.Protocol` for structural subtyping (duck typing with type hints)
- Prefer `dataclasses` or `attrs` for simple data containers
- Use `__slots__` for memory-efficient classes when appropriate
- Keep functions/methods short (<20 lines ideally)
