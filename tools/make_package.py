#!/usr/bin/env python3
"""
tools/make_package.py - builds fdicsweep_package_v<version>.docx: the whole codebase (every git-tracked file at HEAD) as a zip,
base64-encoded into a Word document together with one-liner extract commands (PowerShell and python3).

    python3 tools/make_package.py [--out package] [--ref HEAD]

The version is read from sweep_common.py. Real env files, outputs, logs and reports are not tracked by git, so they are never
included. The result is checked before the script finishes: the python3 one-liner is run on a copy and the extracted files must
equal `git ls-files`. Needs python-docx (requirements-dev.txt).
"""
import argparse
import base64
import os
import re
import subprocess
import sys
import shlex
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHUNK = 2800
START, END = "###START_CODE###", "###END_CODE###"

TITLE = 'Extract via Terminal (Takes 10 Seconds)'
STEP1 = 'Step 1: Open Terminal in the folder where this document is saved.'
STEP2 = 'Step 2: Copy and paste the following one-liner command into Terminal and hit Enter after closing the document'
POWERSHELL = '$d = if (Test-Path "fdicsweep_package_v{V}.docx") { (Resolve-Path "fdicsweep_package_v{V}.docx").Path } else { (Get-Item "fdicsweep_package_v*.docx" | Select-Object -Last 1).FullName }; Add-Type -AssemblyName System.IO.Compression.FileSystem; $z = [IO.Compression.ZipFile]::OpenRead($d); $x = (New-Object IO.StreamReader($z.GetEntry("word/document.xml").Open())).ReadToEnd(); $z.Dispose(); $s = $x.Substring($x.LastIndexOf("###START_CODE###") + 16); $p = $s.Substring(0, $s.IndexOf("###END_CODE###")); $b = -join ([regex]::Matches($p, "<w:t[^>]*>(.*?)</w:t>") | ForEach-Object { $_.Groups[1].Value }); [IO.File]::WriteAllBytes("$pwd\\s.zip", [Convert]::FromBase64String($b)); Expand-Archive "$pwd\\s.zip" -DestinationPath "$pwd\\fdicsweep" -Force; Remove-Item "$pwd\\s.zip"; Write-Host "Done! Extracted into fdicsweep folder" -ForegroundColor Green'
PY_LABEL = 'macOS / Linux alternative (needs python3):'
PYTHON = 'python3 -c "import zipfile,re,base64,glob,os;d=sorted(glob.glob(\'fdicsweep_package_v*.docx\'))[-1];x=zipfile.ZipFile(d).read(\'word/document.xml\').decode();s=x.rindex(\'###START\'+\'_CODE###\')+16;p=x[s:x.index(\'###END\'+\'_CODE###\',s)];b=\'\'.join(re.findall(r\'<w:t[^>]*>(.*?)</w:t>\',p));open(\'s.zip\',\'wb\').write(base64.b64decode(b));zipfile.ZipFile(\'s.zip\').extractall(\'fdicsweep\');os.remove(\'s.zip\');print(\'Done! Extracted into fdicsweep folder\')"'


def version():
    m = re.search(r'__version__\s*=\s*"([^"]+)"', (ROOT / "sweep_common.py").read_text(encoding="utf-8"))
    if not m:
        sys.exit("cannot read __version__ from sweep_common.py")
    return m.group(1)


def git(*args):
    return subprocess.run(["git", "-C", str(ROOT), *args], check=True, capture_output=True).stdout


def build(out_dir, ref="HEAD"):
    from docx import Document
    v = version()
    name = f"fdicsweep_package_v{v}.docx"
    with tempfile.TemporaryDirectory() as tmp:
        zpath = Path(tmp) / "s.zip"
        git("archive", "--format=zip", "-9", "-o", str(zpath), ref)
        payload = base64.b64encode(zpath.read_bytes()).decode()
    doc = Document()
    doc.add_heading(TITLE, 1)
    doc.add_paragraph(STEP1)
    doc.add_paragraph(STEP2)
    doc.add_paragraph(POWERSHELL.replace("{V}", v))
    doc.add_paragraph(PY_LABEL)
    doc.add_paragraph(PYTHON)
    doc.add_heading("Encoded Data Payload", 1)
    doc.add_paragraph(START)
    for i in range(0, len(payload), CHUNK):
        doc.add_paragraph(payload[i:i + CHUNK])
    doc.add_paragraph(END)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / name
    doc.save(target)
    return target


def verify(docx_path, ref="HEAD"):
    """Runs the python3 one-liner of the document on a copy; the extracted files must equal git ls-files."""
    with tempfile.TemporaryDirectory() as tmp:
        copy = Path(tmp) / Path(docx_path).name
        copy.write_bytes(Path(docx_path).read_bytes())
        argv = shlex.split(PYTHON)          # ['python3', '-c', <code>]
        subprocess.run([sys.executable] + argv[1:], cwd=tmp, check=True, capture_output=True)
        got = sorted(str(p.relative_to(Path(tmp) / "fdicsweep")).replace(os.sep, "/")
                     for p in (Path(tmp) / "fdicsweep").rglob("*") if p.is_file())
    want = sorted(git("ls-tree", "-r", "--name-only", ref).decode().splitlines())
    if got != want:
        sys.exit(f"verification failed: {len(got)} extracted files, {len(want)} tracked")
    return len(got)


def main():
    ap = argparse.ArgumentParser(description="Build the encoded docx package of the codebase.")
    ap.add_argument("--out", default=str(ROOT / "package"), help="output folder (default: package/, git-ignored)")
    ap.add_argument("--ref", default="HEAD", help="git ref to package (default HEAD)")
    ap.add_argument("--no-verify", action="store_true")
    a = ap.parse_args()
    path = build(a.out, a.ref)
    n = None if a.no_verify else verify(path, a.ref)
    print(f"{path} ({path.stat().st_size} bytes)" + (f", verified: {n} files extract identically to git" if n is not None else ""))


if __name__ == "__main__":
    main()
