def api_root(version):
    return f"/api/v{version}"


def api_path(name, suffix=None, version=None, base=None):
    if base is None:
        base = api_root(version)
    else:
        if base.startswith('/'):
            base = base[1:]
        base = f"{api_root(version)}/{base}"

    if base.endswith('/'):
        base = base[:-1]
    if suffix is None:
        return f"{base}/{name}"
    else:
        return f"{base}/{name}/{suffix}"


def req_url(name, ip=None, port=None, suffix=None, version=None, base=None):
    return f'http://{ip}:{port}{api_path(name, suffix, version, base)}'
