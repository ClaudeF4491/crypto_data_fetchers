"""
Selenium browser driver support functions including metamask
extension integration and wallet loading.
"""
import logging
from selenium import webdriver
from time import sleep
from typing import Optional, Sequence, Union


def init_driver_firefox(
    executable_path: Optional[str] = None,
    extension_paths: Optional[Sequence[str]] = None,
    headless: bool = True,
) -> webdriver.Firefox:
    """
    Initializes Selenium Firefox webdriver.
    Also optionally installs provided extensions

    Args:
        executable_path: Path to geckodriver executable. Defaults to looking in PATH
        extension_path: Optional list of extension files (e.g. .xpi type)
        headless: Flag to run headless
    Returns:
        driver: Initialized driver
    """
    opts = webdriver.FirefoxOptions()
    opts.headless = headless
    if executable_path:
        logging.debug(
            f"Initializing Firefox driver using executable: {executable_path}"
        )
        driver = webdriver.Firefox(options=opts, executable_path=executable_path)
    else:
        driver = webdriver.Firefox(options=opts)
    if extension_paths:
        for ep in extension_paths:
            logging.debug(f"Initialize extension file: {ep}")
            driver.install_addon(ep)

    return driver


def load_wallet_metamask(
    driver: Union[webdriver.Chrome, webdriver.Firefox],
    wallet_seed: Sequence[str],
    wallet_password: str,
    sleep_sec: Optional[float] = 3.0,
) -> None:
    """
    Loads a wallet into metamask extension.
    Assumes metamask setup page is the active one.

    WARNING: use burner wallets only to be safe

    Args:
        driver: Web driver
        wallet_seed: N-word seed phrase to use for wallet
        wallet_password: Password to keep for wallet in this session
        sleep_sec: Optional duration to sleep between metamask click actions

    Returns:
        None
    """
    # Click setup buttons
    driver.find_element_by_xpath('//button[text()="Get Started"]').click()
    driver.find_element_by_xpath('//button[text()="Import wallet"]').click()
    driver.find_element_by_xpath('//button[text()="No Thanks"]').click()

    # Fill out password page
    inputs = driver.find_elements_by_xpath("//input")
    seed_splits = wallet_seed.split(" ")
    for i, seed_word in enumerate(seed_splits):
        # Cycle through inputs looking for match
        for el in inputs:
            attr_id = el.get_attribute("id")
            if f"word-{i}" in attr_id and "checkbox" not in attr_id:
                # Found match. Use seed word here
                el.send_keys(seed_word)
                break

    # Fill in password
    inputs[-3].send_keys(wallet_password)
    inputs[-2].send_keys(wallet_password)

    # Check the box
    inputs[-1].click()

    # Click import button
    driver.find_element_by_xpath('//button[text()="Import"]').click()

    # Wait a moment and finalize
    sleep(sleep_sec)
    driver.find_element_by_xpath('//button[text()="All Done"]').click()


def load_url_and_connect_metamask(
    driver: Union[webdriver.Chrome, webdriver.Firefox],
    url: str,
    sleep_sec: Optional[float] = 5.0,
) -> None:
    """
    Connect metamask to site by pressing all the connect buttons.
    Assumes when you load the URL, Metamask pops up requesting to
    connect.

    Ar:
        driver: Web driver
        url: URL to load
        sleep_sec: Optional time to wait for Metamask to load pages
    """
    # Load page. On first call this will load metamask to approve/switch networks
    orig_handle = driver.current_window_handle
    driver.get(url)

    # Wait for metamask to pop up, switch to tab and connect
    sleep(sleep_sec)

    # Assumes metamask popped up
    driver.switch_to.window(driver.window_handles[-1])
    driver.find_element_by_xpath('//button[text()="Next"]').click()
    driver.find_element_by_xpath('//button[text()="Connect"]').click()

    # Refresh ... again ... and wait for full first page load
    logging.info("Refreshing page.")
    sleep(sleep_sec)

    # Switch back to original handle
    driver.switch_to.window(orig_handle)
