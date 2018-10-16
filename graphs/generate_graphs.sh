#!/bin/bash
#
# Generate graphs of the documentation.
#
# All graphs are specified in the Mermaid (https://mermaidjs.github.io/)
# format (mmd) and use this nodejs CLI tool to generate the image outputs.
#
# If mermaid is not installed on the system, it will be installed locally
# under node_modules and not added to package.json.
#
# NB: not directly included in the dependencies since it requires chromium
# dependencies (100MB to download...)

files=$(find . -not -path "./node_modules*" -type f -name "*.mermaid")
extension=${1:-"png"}
mmdc_params="${@:2}"

# Mermaid cli installation if necessary
mmdc=$(command -v mmdc)
if ! [ -x "${mmdc}" ]; then
    # Check if existing as local dependency
    mmdc="./node_modules/.bin/mmdc"
    if ! [ -x "${mmdc}" ]; then
       npm install --no-save mermaid.cli
       if [ $? -eq 0 ]; then
           echo "Failed to install mermaid.cli"
          exit 1
       fi
    fi
fi

# Generate the graphs
for ifile in ${files}
do
    ofile="${ifile}.${extension}"
    echo "Generating graph ${ofile}"
    ${mmdc} -i "${ifile}" -o "${ofile}" ${mmdc_params}
done
