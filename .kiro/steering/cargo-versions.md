---
inclusion: always
---

# Cargo.toml Version Policy

## Critical Rule: Do Not Modify Dependency Versions

When working with Cargo.toml files in this project, you MUST follow this rule:

**NEVER change the version numbers of dependencies that are already specified in Cargo.toml files.**

### Why This Matters

- Dependency versions are carefully selected for compatibility
- Changing versions can introduce breaking changes
- Version updates require thorough testing across the entire codebase
- The project may rely on specific features or behaviors of particular versions

### What You Can Do

✅ **Allowed:**
- Add new dependencies (with appropriate versions)
- Add features to existing dependencies
- Modify feature flags
- Update project metadata (description, authors, etc.)
- Add or modify build configurations

❌ **Not Allowed:**
- Changing version numbers of existing dependencies
- Upgrading or downgrading dependency versions
- Modifying version constraints (e.g., changing "1.0" to "1.5")

### Example

If you see:
```toml
rayon = "1.11"
```

Do NOT change it to:
```toml
rayon = "1.12"  # ❌ WRONG
```

### When Versions Need to Change

If you believe a version needs to be updated:
1. Stop and ask the user first
2. Explain why the version change is necessary
3. Wait for explicit approval before making the change

This policy ensures stability and prevents unexpected breaking changes.
