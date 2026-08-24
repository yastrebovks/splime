# SPLime 0.4.9

SPLime 0.4.9 corrects the Public Object execution architecture introduced in
0.4.8. PyPI contains only the `splime` distribution. Signed Public runtime
locks carry exact Object dependency wheels and require an installed compatible
framework; they do not carry or install a second SPLime execution package.

The isolated Public environment receives a hash-bound projection of the
caller's installed `spl` package. Dependency wheels that declare `splime` or
project files onto `spl`—including through wheel `.data` directories—are
rejected, as are `.pth`, `sitecustomize`, and `usercustomize` startup hooks.
Existing framework, daemon,
notebook, YAML/IR, Library, Adapter, Run, private execution, and private
`NodeRemote` behavior remains unchanged.

The compatibility evidence consists of the existing 18-version/36-artifact
0.4.8 matrix plus a separately hash-bound probe of published 0.4.8, for all 19
actually published predecessor versions.

```bash
python3.13 -m pip install --upgrade "splime==0.4.9"
```
