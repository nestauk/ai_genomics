"""ai_genomics/utils/io.py
A set of functions for fetching remote files.
"""
from ai_genomics.utils.reading import make_path_if_not_exist
from io import BytesIO
import logging
import pathlib
import requests
import tqdm
from typing import Union


logger = logging.getLogger(__name__)


def fetch(
    url: str,
    fout: Union[pathlib.Path, str] = None,
    timeout: int = 10,
):
    """Downloads an object from a url.

    Args:
        url: Url of object to retrieve.
        fout: Path to save object. If `None`, then the item is returned as a
            bytes object.

    Returns:
        bio: BytesIO of retrieved object if `fout` is not None.
    """
    logger.info(f"Downloading {url}")

    if fout is not None:
        path = "/".join(str(fout).split("/")[:-1])
        make_path_if_not_exist(path)
        with open(fout, "wb") as f:
            for chunk in stream(url, timeout=timeout):
                f.write(chunk)
    else:
        bio = BytesIO()
        for chunk in stream(url, timeout=timeout):
            bio.write(chunk)
        return bio


def stream(
    url: str,
    timeout: int = None,
):
    """Streams a file from a url in 1MB chunks."""
    chunk_size = 1024 * 1024
    resp = requests.get(url, stream=True, timeout=timeout)
    total = int(resp.headers.get("content-length", 0))
    with tqdm.tqdm(
        desc=url,
        total=total,
        unit="b",
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            bar.update(len(chunk))
            yield chunk
