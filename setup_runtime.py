"""Cache JVM dependencies inside the project; never change system settings."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import urllib.request

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / '.runtime'
MAVEN = 'https://repo.maven.apache.org/maven2'
JARS = {
    'delta-spark_2.12-3.3.2.jar': 'io/delta/delta-spark_2.12/3.3.2',
    'delta-storage-3.3.2.jar': 'io/delta/delta-storage/3.3.2',
    'antlr4-runtime-4.9.3.jar': 'org/antlr/antlr4-runtime/4.9.3',
}
WINUTILS_COMMIT = '7386986d5d8a079b5cd4464f4599766dd27e7d13'
SHA256 = {
    'delta-spark_2.12-3.3.2.jar': '4e92085d5d794cf949c50bb665b91384857cad58680c2d0bfc9c7ad353e98e85',
    'delta-storage-3.3.2.jar': 'c096975a2ca97ccc9b4005aa5cce16c9ac3903139d340da6665b66ace67d7c36',
    'antlr4-runtime-4.9.3.jar': '131a6594969bc4f321d652ea2a33bc0e378ca312685ef87791b2c60b29d01ea5',
    'winutils.exe': '496a591eb1e67df2a620f710d529ba6ddfe1c19149e6647cc4e320bb0efd8553',
    'hadoop.dll': 'd7ab36a68518748cef142be2da5069b4c763c2cd764c1d2e6ac48c7200405be3',
}


def fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={'User-Agent': 'ecommerce-local-setup/1.0'})
    with urllib.request.urlopen(request, timeout=90) as response:
        return response.read()


def main() -> None:
    RUNTIME.mkdir(exist_ok=True)
    manifest_path = RUNTIME / 'downloads.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    for name, folder in JARS.items():
        path = RUNTIME / 'jars' / name
        path.parent.mkdir(exist_ok=True)
        url = f'{MAVEN}/{folder}/{name}'
        if not path.exists():
            print(f'Downloading {name}', flush=True)
            payload = fetch(url)
            expected = fetch(url + '.sha1').decode().strip().split()[0]
            if hashlib.sha1(payload).hexdigest() != expected:
                raise RuntimeError(f'Maven checksum mismatch: {name}')
            path.write_bytes(payload)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != SHA256[name]:
            raise RuntimeError(f'Pinned SHA-256 mismatch: {path}')
        previous = manifest.get(name)
        if previous and previous['sha256'] != digest:
            raise RuntimeError(f'Cached dependency changed: {path}')
        manifest[name] = {'url': url, 'sha256': digest}
    if os.name == 'nt':
        # Apache does not publish Windows native binaries. Pin the community
        # build to one immutable git commit and retain its provenance/checksums.
        commit = WINUTILS_COMMIT
        manifest['winutils_commit'] = commit
        for name in ('winutils.exe', 'hadoop.dll'):
            path = RUNTIME / 'hadoop' / 'bin' / name
            path.parent.mkdir(parents=True, exist_ok=True)
            url = f'https://raw.githubusercontent.com/cdarlint/winutils/{commit}/hadoop-3.3.6/bin/{name}'
            if not path.exists():
                print(f'Downloading Windows Hadoop helper {name}', flush=True)
                path.write_bytes(fetch(url))
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != SHA256[name]:
                raise RuntimeError(f'Pinned SHA-256 mismatch: {path}')
            previous = manifest.get(name)
            if previous and previous['sha256'] != digest:
                raise RuntimeError(f'Cached dependency changed: {path}')
            manifest[name] = {'url': url, 'sha256': digest}
    manifest_path.write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    print(f'Runtime ready: {RUNTIME}', flush=True)


if __name__ == '__main__':
    main()
