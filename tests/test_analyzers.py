import json
import os
import tempfile
from pathlib import Path

from gg.analyzers.dependencies import analyze_dependencies
from gg.analyzers.languages import analyze_languages
from gg.analyzers.structure import analyze_structure


def _make_project(tmp: Path, files: dict[str, str]) -> Path:
    for rel_path, content in files.items():
        full = tmp / rel_path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content)
    return tmp


class TestLanguageAnalyzer:
    def test_detect_python(self, tmp_path):
        _make_project(tmp_path, {
            "app.py": "print('hello')",
            "utils/helpers.py": "def helper(): pass",
        })
        result = analyze_languages(tmp_path)
        assert result.primary_language == "Python"
        assert result.languages["Python"] == 2
        assert result.total_files == 2

    def test_detect_typescript(self, tmp_path):
        _make_project(tmp_path, {
            "src/index.ts": "export {}",
            "src/app.tsx": "export default App",
            "package.json": "{}",
        })
        result = analyze_languages(tmp_path)
        assert result.primary_language == "TypeScript"

    def test_detect_framework_nextjs(self, tmp_path):
        _make_project(tmp_path, {
            "next.config.js": "module.exports = {}",
            "pages/index.tsx": "export default Home",
        })
        result = analyze_languages(tmp_path)
        assert "Next.js" in result.frameworks

    def test_detect_tailwind(self, tmp_path):
        _make_project(tmp_path, {
            "tailwind.config.js": "module.exports = {}",
            "src/app.tsx": "",
        })
        result = analyze_languages(tmp_path)
        assert "Tailwind CSS" in result.styling

    def test_empty_project(self, tmp_path):
        result = analyze_languages(tmp_path)
        assert result.primary_language == "Unknown"
        assert result.total_files == 0

    def test_skips_node_modules(self, tmp_path):
        _make_project(tmp_path, {
            "src/app.py": "",
            "node_modules/pkg/index.js": "",
        })
        result = analyze_languages(tmp_path)
        assert result.primary_language == "Python"
        assert "JavaScript" not in result.languages


class TestDependencyAnalyzer:
    def test_parse_package_json(self, tmp_path):
        _make_project(tmp_path, {
            "package.json": json.dumps({
                "dependencies": {"react": "^18.0.0"},
                "devDependencies": {"jest": "^29.0.0"},
            }),
        })
        result = analyze_dependencies(tmp_path)
        assert result.package_manager == "npm"
        assert "react" in result.runtime_deps
        assert "jest" in result.dev_deps

    def test_detect_yarn(self, tmp_path):
        _make_project(tmp_path, {
            "package.json": json.dumps({"dependencies": {}}),
            "yarn.lock": "",
        })
        result = analyze_dependencies(tmp_path)
        assert result.package_manager == "yarn"

    def test_detect_pnpm(self, tmp_path):
        _make_project(tmp_path, {
            "package.json": json.dumps({"dependencies": {}}),
            "pnpm-lock.yaml": "",
        })
        result = analyze_dependencies(tmp_path)
        assert result.package_manager == "pnpm"

    def test_detect_existing_eslint(self, tmp_path):
        _make_project(tmp_path, {
            "package.json": json.dumps({"devDependencies": {"eslint": "^8.0"}}),
        })
        result = analyze_dependencies(tmp_path)
        assert "linters" in result.existing_tools
        assert "eslint" in result.existing_tools["linters"]

    def test_unknown_project(self, tmp_path):
        result = analyze_dependencies(tmp_path)
        assert result.package_manager == "unknown"


class TestStructureAnalyzer:
    def test_classify_dirs(self, tmp_path):
        _make_project(tmp_path, {
            "src/main.py": "",
            "tests/test_main.py": "",
            "docs/readme.md": "",
        })
        result = analyze_structure(tmp_path)
        assert result.classifications.get("src") == "source"
        assert result.classifications.get("tests") == "tests"
        assert result.classifications.get("docs") == "docs"

    def test_detect_monorepo_lerna(self, tmp_path):
        _make_project(tmp_path, {
            "lerna.json": "{}",
            "packages/a/index.js": "",
        })
        result = analyze_structure(tmp_path)
        assert result.is_monorepo is True

    def test_detect_monorepo_workspaces(self, tmp_path):
        _make_project(tmp_path, {
            "package.json": json.dumps({"workspaces": ["packages/*"]}),
        })
        result = analyze_structure(tmp_path)
        assert result.is_monorepo is True

    def test_top_level_dirs(self, tmp_path):
        _make_project(tmp_path, {
            "src/app.py": "",
            "tests/test.py": "",
            "scripts/build.sh": "",
        })
        result = analyze_structure(tmp_path)
        assert "src" in result.top_level_dirs
        assert "tests" in result.top_level_dirs
        assert "scripts" in result.top_level_dirs
