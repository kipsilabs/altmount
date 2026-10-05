# Nested NZB fixtures

`compressed.rar` and `encrypted.rar` are compressed RAR5 archives containing
`release.nzb`, which references the fake article `nested-media-p001@battery`.
The encrypted fixture protects both headers and content with `wrapper-secret`.
`solid.rar` places a compressed `.nfo` before the NZB to exercise solid decoding.
`multi.part1.rar` through `multi.part3.rar` contain the same NZB with a
deterministic XML comment added to exercise decompression across volume boundaries.
Tests use committed archives and do not require the RAR CLI.

To regenerate from this directory using the RAR CLI:

```sh
rar a -y -m5 -md1m compressed.rar release.nzb
rar a -y -m5 -md1m -hpwrapper-secret encrypted.rar release.nzb
python3 -c 'print("Nested NZB wrapper test.\n" * 20, end="")' > sidecar.nfo
rar a -y -m5 -md1m -s -ds solid.rar sidecar.nfo release.nzb
rm sidecar.nfo
```

To regenerate the multipart fixture:

```sh
python3 - <<'PY'
import base64
import pathlib
import random
import subprocess
import tempfile

root = pathlib.Path.cwd()
data = root.joinpath("release.nzb").read_bytes()
comment = base64.b64encode(random.Random(952).randbytes(8000))
with tempfile.TemporaryDirectory() as work:
    pathlib.Path(work, "release.nzb").write_bytes(data + b"<!--" + comment + b"-->")
    subprocess.run(["rar", "a", "-y", "-m5", "-md1m", "-v4k",
                    str(root / "multi.rar"), "release.nzb"], cwd=work, check=True)
PY
```
