#!/bin/bash

set -ex

version="v2"
WCA_PUB_URL="https://www.worldcubeassociation.org/export/results"
WCA_PERMA_URL="https://www.worldcubeassociation.org/export/results/${version}/tsv"

[[ -d "$1" ]] || (echo "$1 is not a dir" >&2 && exit 1)

workdir="$1/$(date +%Y/%m/%d)"
mkdir -p "$workdir"
# pub_html_file="$workdir/results.html"
# 
# if [[ ! -e "$pub_html_file" ]];then
#     curl -sSL "$WCA_PUB_URL" -o "$pub_html_file"
# fi

link_file="$workdir/permalink.$version.head.txt"
if [[ ! -e "$link_file" ]];then
    curl -I "$WCA_PERMA_URL" -o "$link_file"
fi

download_url="$(grep -Eo 'https://[^"]+.tsv.zip' "$link_file")"

[[ -z "$download_url" ]] && echo "cannot retrieve download url from html" && exit 1

zip_file_name="$(basename "$download_url")"

[[ "$zip_file_name" =~ .tsv.zip ]] || (echo "invalid tsv zip file: $zip_file_name" && exit 1)

[[ "$zip_file_name" =~ $(date +%Y%m%d)T ]] || (echo "not today's tsv zip file: $zip_file_name" && exit 1)

zip_file_path="$workdir/$zip_file_name"

if [[ ! -e "$zip_file_path" ]];then
    curl -sSL "$download_url" -o "$zip_file_path"
fi

