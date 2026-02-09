import os
import sys

try:
    import pathspec
except ImportError:
    print("Error: pathspec is not installed")
    print("Run: pip install pathspec")
    sys.exit(1)


def load_gitignore(directory):
    """
    Reads .gitignore and returns a pathspec object for matching.
    """
    gitignore_path = os.path.join(directory, ".gitignore")

    if not os.path.exists(gitignore_path):
        return None

    with open(gitignore_path, "r", encoding="utf-8") as f:
        patterns = f.read().splitlines()

    # Filter empty lines and comments
    patterns = [p for p in patterns if p.strip() and not p.startswith("#")]

    return pathspec.PathSpec.from_lines("gitwildmatch", patterns)


def generate_tree(
    directory,
    prefix="",
    excluded=None,
    ignored_dirs=None,
    gitignore_spec=None,
    project_root=None
):
    """
    Generates a tree-like structure of the project.
    """
    if excluded is None:
        excluded = set()
    if ignored_dirs is None:
        ignored_dirs = set()
    if project_root is None:
        project_root = directory

    content = []

    try:
        items = sorted(os.listdir(directory))
    except PermissionError:
        return content

    # Basic exclusions
    items = [
        i for i in items
        if i not in excluded
        and i not in ignored_dirs
        and not i.endswith(".spec")
    ]

    visible_items = []
    for name in items:
        path = os.path.join(directory, name)

        if gitignore_spec:
            rel_path = os.path.relpath(path, project_root).replace(os.sep, "/")

            if gitignore_spec.match_file(rel_path):
                continue

            if os.path.isdir(path) and gitignore_spec.match_file(rel_path + "/"):
                continue

        visible_items.append(name)

    for index, name in enumerate(visible_items):
        path = os.path.join(directory, name)
        is_last = index == len(visible_items) - 1
        connector = "└── " if is_last else "├── "

        if os.path.isdir(path):
            content.append(f"{prefix}{connector}{name}/")
            extension = "    " if is_last else "│   "
            content.extend(
                generate_tree(
                    path,
                    prefix + extension,
                    excluded,
                    ignored_dirs,
                    gitignore_spec,
                    project_root
                )
            )
        else:
            content.append(f"{prefix}{connector}{name}")

    return content


def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Manual exclusions (minimal; .gitignore does most of the work)
    current_script = os.path.basename(__file__)
    excluded = {current_script}

    # Always ignored directories
    ignored_dirs = {".git"}

    # Load .gitignore patterns
    gitignore_spec = load_gitignore(project_root)

    if gitignore_spec:
        print("Loaded .gitignore patterns successfully")
    else:
        print("Warning: .gitignore not found, processing all files")

    # Generate tree
    tree = generate_tree(
        project_root,
        excluded=excluded,
        ignored_dirs=ignored_dirs,
        gitignore_spec=gitignore_spec,
        project_root=project_root
    )

    tree_output = "\n".join(tree)

    # Write treemap.md
    agent_dir = os.path.join(project_root, "agent")
    analysis_dir = os.path.join(agent_dir, "analysis")
    os.makedirs(analysis_dir, exist_ok=True)
    output_file = os.path.join(analysis_dir, "treemap.md")
    existed = os.path.exists(output_file)

    with open(output_file, "w", encoding="utf-8") as f:
        # Project structure (derived)
        f.write("## Project Structure\n\n```\n")
        f.write(tree_output)
        f.write("\n```\n\n")

        # Declarative notes (non-derived)
        f.write("## Cache Artifacts\n\n")
        f.write("- path_cache.json: single runtime cache file\n")
        f.write("- Host-specific or suffixed cache files are not supported\n")
        f.write("- Any additional cache files should be considered accidental artifacts\n")

    if existed:
        print("treemap.md updated successfully")
    else:
        print("treemap.md created for the first time")

    return 0


if __name__ == "__main__":
    sys.exit(main())
