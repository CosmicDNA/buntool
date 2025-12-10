# BunTool
<p align="center">
  <img src="static/buntool.webp" width="300" style="center">
</p>

Automatically make court bundles in seconds.  Check out the main instance: [buntool.co.uk](https://buntool.co.uk)

Takes input PDF files; generates index data; outputs a merged PDF with index, hyperlinks, bookmarks and page numbers according to your chosen settings.

Output Bundles comply with the requirements of the English Courts, and are also useful for a range of other applications.

# Usage and installation

This is configured for self-hosting, which is what these isntructions are for.


```bash
# 1. Create the virtual environment (a hidden folder named .venv)
python3 -m venv .venv

# 2. Activate the virtual environment
source .venv/bin/activate

# 3. Install the required packages
pip install -e .
```

## Ready to bake

Now you can start the server:
```bash
buntool
```
Then visit `http://127.0.0.1:7001` in your browser.

# Copyright and License

Copyright © Tristan Sherliker and contributors to BunTool

Licensed under the Mozilla Public License, version 2.0.