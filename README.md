# crypto_data_fetchers
A collection of data fetchers using public crypto APIs.
The adapters contains the API adapters, source code, and examples at the
bottoms of the modules.

The scripts includes single-execution scripts to download all history.
They also include scripts that poll periodically and report results
to a data sink.

## Setup
1. Create virtual environment
1. Install requirements with: `pip install -r requirements.txt`
2. Look in `scripts` directory for available features.

## Alpha Homora Selenium Scraper

The data extracted from `alpha_homora.py` is from the Alpha Homora
backend APIs that are called in page load. However, that data is
pretty raw and the frontend performs a LOT of calculations in
order to provide real APYs, borrow rates, find max leverage,
etc.

So this scraper uses Selenium and the Chrome Web Driver in
order to fetch the data. See `alpha_homora_scrape.py`.
It's less robust and slower, but it is more accurate and
detailed data. Selenium + ChromeDriver require operating-system
specific instructions to set up.

## Setup

1. Download geckodriver from [here](https://github.com/mozilla/geckodriver/releases).
  1. Choose your operating system package in the release section
  1. Unpack, and copy to your PATH directory (e.g. `/usr/local/bin/)
  1. Provide executable permissions for it, e.g. `chmod a+x`
1. Download Metamask Firefox Addon from [here](https://addons.mozilla.org/en-US/firefox/addon/ether-metamask/)
  1. On that page, choose `Download file`. Copy it to this directory.
1. Update configuration to point to the *full path* of the downloaded file (e.g. `metamask-10.12.4-an+fx.xpi`)
