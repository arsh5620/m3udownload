#!/usr/bin/python3
import argparse
import os
import urllib.parse
import sys
import requests
import threading
import subprocess


def print_banner():
    help="""A simple script to download playlist/HLS files
    
Common usage scenario: `download.py --temp-folder=temp --base-url='https://some-website.com/video-api-or-something/' --out=final.mp4`

The program expects to either find a `index.m3u8` file in the current folder or `--index=file` argument
The base URL must be a fully specified url with protocol information
If the program is interrupted while downloading, you can do a `--retry` but that is not supported fully"""

    print(help)

def validate_url(x):
    try:
        result = urllib.parse. urlparse(x)
        return all([result.scheme, result.netloc])
    except:
        return False


def get_proper_urls(base_url, index_contents, additional_args):
    index_lines = index_contents.splitlines()
    urls = []
    for index_line in index_lines:
        segment_name = index_line.strip()
        if not segment_name.startswith("#"):
            url = urllib.parse.urljoin(base_url, segment_name, additional_args)
            urls.append(url)

    return urls


def download_url(url, filename):
    try:
        request = requests.get(url)
        if (len(request.content) == 0):
            print("Error occured while downloading, server responded with empty packet")
            return False

        open(filename, 'wb').write(request.content)
        return True
    except Exception as e:
        print("Error occured while downloading: '{}'".format(e))
        return False


def worker(semaphore, content, index):
    with semaphore:
        print("Currently downloading '{}' into '{}'".format(content, index))

        while not download_url(content, index):
            print("Failed to download '{}', trying again".format(content))


def generate_ffmpeg_sources_list(indexes):
    ffmpeg_contents = ""
    for index in indexes:
        ffmpeg_contents += "file '{}'\n".format(os.path.normpath(index))

    with open("ffmpeg_m3u8_sources.txt", "w") as ffmpeg_file:
        ffmpeg_file.write(ffmpeg_contents)


def run_ffmpeg_copy(output_filename):
    # ffmpeg -f concat -i mylist.txt -c copy all.ts
    print("Running final steps, doing ffmpeg copy of all the temp files")
    subprocess.run(["ffmpeg", "-f", "concat",
                   "-i", "ffmpeg_m3u8_sources.txt", "-c", "copy", output_filename])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download M3U8 HLS files')
    parser.add_argument("--temp-folder", dest="tempfolder", action="store", default=".",
                        help='The folder to use for downloading temp files before the join phase')
    parser.add_argument("--index", dest="index", action="store",
                        default="index.m3u8", help="The index file to download the contents from")
    parser.add_argument("--args", dest="additionalargs", action="store",
                        default="", help="Additional arguments, will be passed as-is to the network request")
    parser.add_argument("--out", dest="outfile", action="store",
                        default="", help="Output file name is mandatory")
    parser.add_argument("--retry", dest="retry", action="store_true",
                        default="", help="In case there are any missing files that weren't downloaded last time, try from there")
    parser.add_argument("--base-url", dest="baseurl", action="store",
                        help="The base url from which to fetch index's sources")

    args = parser.parse_args()

    if not len(sys.argv) > 1:
        print_banner()
        exit(1)

    if not args.outfile:
        print("Output file is not provided, try `--out=final.mp4` or similar")
        exit(1)

    if not args.baseurl:
        print("Base URL was not set and is required to be able to download the index's sources, try `--help` for more information.", file=sys.stderr)
        exit(1)

    if not validate_url(args.baseurl):
        print("Given base URL is not valid '{}'".format(args.baseurl))
        exit(1)

    if not args.baseurl.strip().endswith("/"):
        print("Base URL must end with a '/'")
        exit(1)

    with open(args.index, "r") as index_file:
        index_contents = index_file.read()

    if not index_contents:
        print("Cannot read index contents from '{}'".format(args.index))
        exit(1)

    content_sources = get_proper_urls(
        args.baseurl, index_contents, args.additionalargs)

    print("Downloading indexes")

    index_counter = 0
    indexes = []

    if not os.path.exists(args.tempfolder):
        print("Creating temp directory '{}'".format(args.tempfolder))
        os.makedirs(args.tempfolder)

    concurrency_semaphore = threading.Semaphore(8)
    threads = []

    for content in content_sources:
        index = os.path.join(
            args.tempfolder, "index-temp-{}.m3uindex".format(index_counter))
        indexes.append(index)

        if (args.retry and (not os.path.exists(index) or os.stat(index).st_size == 0)) or not args.retry:
            thread = threading.Thread(target=worker, name=str(
                content), args=(concurrency_semaphore, content, index))
            thread.start()
            threads.append(thread)

        index_counter += 1

    for thread in threads:
        thread.join()

    generate_ffmpeg_sources_list(indexes)

    run_ffmpeg_copy(args.outfile)

    print("========")
    print("FINISHED")
    print("========")
    print("ffmpeg copy has finished producing output file '{}', you will need clean the temp directory '{}'".format(
        args.outfile, args.tempfolder))
