import logging
import csv, json, os
import boto3

from logging import INFO
from time import sleep
from datetime import datetime
from botocore.exceptions import ClientError

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import selenium.common.exceptions as Exceptions

WMS_URL = "https://wms.bolt.eu"
DOWNLOAD_DIR = "/tmp"
WAIT_TIME = 10

options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--start-maximized")
options.add_argument("--no-sandbox")
options.add_argument("--disable-gpu")
options.add_argument("--ignore-certificate-errors")
options.add_argument("--ignore-ssl-errors")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--remote-debugging-port=9222")

options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option("useAutomationExtension", False)
options.add_experimental_option(
    "prefs",
    {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "profile.default_content_settings": {"images": 2},
    },
)

class ScrapperException(Exception): pass

logger = logging.getLogger(__name__)
logger.setLevel(level=INFO)

session = boto3.session.Session()
sm_client = session.client(
    service_name='secretsmanager',
    region_name='eu-central-1'
)

try:
    response = sm_client.get_secret_value(
        SecretId='BoltPo-Robot'
    )
    secrets = json.loads(response['SecretString'])
except ClientError as e:
    logger.critical(f'Error getting secret: {str(e)}')
    reply = {
            "function_name": "Scrapper",
            "error_message": f"Secrets Manager Error: {str(e)}",
            "error_details": None
        }
    raise ScrapperException(reply)

WMS_USER = secrets["WMS_USER"]
WMS_PASS = secrets["WMS_PASS"]
AWS_ACCESS_KEY_ID = secrets["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = secrets["AWS_SECRET_ACCESS_KEY"]

def handler(event, context):
    try:
        driver = webdriver.Chrome(
            options=options
        )  # no service needed here since we work with selenium image
        logger.info("Headless Chrome initialized")
    except Exception as err:
        logger.critical(f"Chrome initialization error: {str(err)}")
        reply = {
                "function_name": "Scrapper",
                "error_message": f"Chrome initialization error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)
    sleep(2)

    driver.get(WMS_URL)
    sleep(5)
    logger.info("Web Site acquired")

    # login into the wms app
    try:
        WebDriverWait(driver, WAIT_TIME).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input#username"))
        ).send_keys(WMS_USER)

        WebDriverWait(driver, WAIT_TIME).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    '//*[@id="root"]/div/div[1]/div/div[2]/div/form/div[2]/div/input',
                )
            )
        ).send_keys(WMS_PASS)

        btn_login = driver.find_element(
            By.XPATH, '//*[@id="root"]/div/div[1]/div/div[2]/div/form/button'
        )
        sleep(1)

        btn_login.click()
    except Exceptions.NoSuchElementException:
        logger.critical("Authentication fields problem. Abort")
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": "Authentication fields problem. Abort",
                "error_details": None
            }
        raise ScrapperException(reply)

    logger.info("Login successful")

    # select Delivery Options page
    try:
        WebDriverWait(driver, WAIT_TIME).until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    '//*[@id="main-content-box"]/div/aside/div[2]/div/div[1]/ul[1]/li[8]/a',
                )
            )
        ).click()
    except Exceptions.TimeoutException:
        logger.critical("Delivery Orders link not visible in time.")
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": "Delivery Orders link not visible in time. Abort",
                "error_details": None
            }
        raise ScrapperException(reply)
    sleep(1)

    logger.info("Delivery orders selected")

    # select Tomorrow and later tab
    try:
        tabTomorrow_element = driver.find_element(
            By.XPATH, '//*[@id="main-content-box"]/div/div[2]/div/div[2]/div/div[3]/div'
        )
        if tabTomorrow_element.get_attribute("innerHTML") == "Tomorrow and later":
            tabTomorrow_element.click()
        else:
            logger.critical("Tomorrow and later tab not found")
            driver.quit()
            reply = {
                    "function_name": "Scrapper",
                    "error_message": "Tomorrow and later tab not found. Abort",
                    "error_details": None
                }
            raise ScrapperException(reply)
    except Exception as err:
        logger.critical(err)
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)
    sleep(1)

    logger.info("Tab Tomorrow and later selected")

    # generate and download the mov data
    mov_data = []

    store_container = driver.find_element(
        By.XPATH,
        '//*[@id="main-content-box"]/div/div[2]/div/div[3]/div[1]/div/div/div[1]/div/div/span/button',
    )
    store_container.click()

    store_list = driver.find_elements(
        By.XPATH, '//div[@id="store-select-menu"]/div/div/ul/li'
    )
    sleep(1)
    nr_stores = len(store_list)

    driver.find_element(
        By.XPATH, '//*[@id="main-content-box"]/div/div[2]/div/div[1]/h3'
    ).click()

    for i in range(nr_stores):
        if i != 0:  # start with default store
            store_container.click()
            sleep(1)
            store_list = driver.find_elements(
                By.XPATH, '//div[@id="store-select-menu"]/div/div/ul/li'
            )
            store_list[i].click()
            sleep(1)
            store_list = driver.find_elements(
                By.XPATH, '//div[@id="store-select-menu"]/div/div/ul/li'
            )
            store_list[i - 1].click()
            sleep(1)
            driver.find_element(
                By.XPATH, '//*[@id="main-content-box"]/div/div[2]/div/div[1]/h3'
            ).click()

        nr_orders = 0
        row_elements = driver.find_elements(By.XPATH, "//tr")
        for row in row_elements:
            if row.get_attribute("data-key") is not None:
                col_elements = row.find_elements(By.XPATH, ".//td")
                supplier = (
                    col_elements[2]
                    .find_element(By.XPATH, ".//a/div/div/div")
                    .get_attribute("innerHTML")
                )
                store = (
                    col_elements[6]
                    .find_element(By.XPATH, ".//a/div")
                    .get_attribute("innerHTML")
                )
                mov_element = col_elements[7].find_element(By.XPATH, ".//a/div")
                if mov_element.get_attribute("innerHTML")[0] == "<":
                    mov = mov_element.get_attribute("innerHTML").split(">")[-1]
                    has_order = False
                else:
                    mov = mov_element.get_attribute("innerHTML")
                    has_order = True

                mov_data.append([supplier, store, has_order, mov])
            logger.info(f"Store {i} has {nr_orders} elements")
    logger.info("MOV data generated")

    # save the mov_data file to temporary location until transfer to S3
    with open("/tmp/mov_data.csv", "a", newline="") as csv_file:
        writer = csv.writer(csv_file)
        for line in mov_data:
            writer.writerow(line)

    logger.info("MOV file saved")

    # generate report modal window
    try:
        btn_Report = driver.find_element(
            By.XPATH, '//*[@id="main-content-box"]/div/div[2]/div/div[1]/div/button[1]'
        )
        btn_Report.click()
    except Exception as err:
        logger.critical(str(err))
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)
    sleep(1)

    try:
        generate_page = driver.find_element(By.XPATH, "/html/body/div[4]/div[3]/div")
    except Exceptions.NoSuchElementException:
        logger.critical("Cannot generate report modal window")
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"Cannot generate report modal window. Abort",
                "error_details": None
            }
        raise ScrapperException(reply)
    sleep(1)

    logger.info("Generate report page launched")

    # select Bulk PO export
    try:
        rad_elements = generate_page.find_elements(By.XPATH, './/input[@type="radio"]')
        for element in rad_elements:
            if element.get_attribute("value") == "bulk_po":
                element.click()
                break
    except Exception as err:
        logger.critical(err)
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)

    logger.info("Bulk OP selected")

    # select export type
    try:
        generate_page.find_element(
            By.XPATH,
            './/button[contains(@id, "kalep-select-react-") and contains(@id, "-toggle-button")]',
        ).click()
        sleep(1)
        driver.find_element(By.XPATH, '//*[text()="XLSX"]').click()
    except Exception as err:
        logger.critical(err)
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)

    sleep(1)

    logger.info("File format selected")

    # Select report fields
    try:
        checkbox_elements = driver.find_elements(By.XPATH, '//input[@type="checkbox"]')
        for element in checkbox_elements:
            if element.get_attribute("name") == "Select all" and element.is_selected():
                element.click()
            if (
                element.get_attribute("name") == "po_number"
                and not element.is_selected()
            ):
                element.click()
            if (
                element.get_attribute("name") == "store_address"
                and element.is_selected()
            ):
                element.click()
            if element.get_attribute("name") == "price" and element.is_selected():
                element.click()
            if (
                element.get_attribute("name") == "product_name"
                and not element.is_selected()
            ):
                element.click()
            if (
                element.get_attribute("name") == "provicer_id"
                and not element.is_selected()
            ):
                element.click()
            if (
                element.get_attribute("name") == "plan_qty"
                and not element.is_selected()
            ):
                element.click()
            if element.get_attribute("name") == "fact_qty" and element.is_selected():
                element.click()
            if (
                element.get_attribute("name") == "supplier_name"
                and not element.is_selected()
            ):
                element.click()
            if element.get_attribute("name") == "supplier_id" and element.is_selected():
                element.click()
            if (
                element.get_attribute("name") == "edi_store_code"
                and element.is_selected()
            ):
                element.click()
            if element.get_attribute("name") == "unit" and not element.is_selected():
                element.click()
            if (
                element.get_attribute("name") == "edi_supplier_code"
                and element.is_selected()
            ):
                element.click()
            if (
                element.get_attribute("name") == "bolt_sku"
                and not element.is_selected()
            ):
                element.click()
            if (
                element.get_attribute("name") == "delivery_date"
                and not element.is_selected()
            ):
                element.click()
            if (
                element.get_attribute("name") == "created_date"
                and element.is_selected()
            ):
                element.click()
            if (
                element.get_attribute("name") == "supplier_sku"
                and not element.is_selected()
            ):
                element.click()
            if element.get_attribute("name") == "total_sum" and element.is_selected():
                element.click()
            if (
                element.get_attribute("name") == "store_name"
                and not element.is_selected()
            ):
                element.click()
            if element.get_attribute("name") == "ean" and not element.is_selected():
                element.click()
            if (
                element.get_attribute("name") == "total_with_vat_sum"
                and element.is_selected()
            ):
                element.click()
    except Exception as err:
        logger.critical(str(err))
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)

    logger.info("Report tags checked")

    # select location
    try:
        generate_page.find_element(By.ID, "city-multi-select-toggle-button").click()
        sleep(2)

        clj = driver.find_element(By.XPATH, '//*[text()="Cluj-Napoca"]')
        sleep(2)
        driver.execute_script("arguments[0].click();", clj)

        buc = driver.find_element(By.XPATH, '//*[text()="Bucharest"]')
        sleep(1)
        driver.execute_script("arguments[0].click();", buc)

        # hover and click to get rid of the floating list
        dummy = generate_page.find_element(By.XPATH, '//span[text()="Generate report"]')
        hover = ActionChains(driver).move_to_element(dummy)
        hover.click().perform()

    except Exception as err:
        logger.critical(err)
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)

    logger.info("Location selected")

    # select store
    try:
        btn_store = driver.find_element(
            By.XPATH, '//*[@id="storeSelectBox"]/div/div/div/div/span/button'
        )
        driver.execute_script("arguments[0].click();", btn_store)
        sleep(1)

        store_list = generate_page.find_elements(
            By.XPATH, '//div[@id="store-select-menu"]/div/div/ul/li'
        )
        for item in store_list:
            item.click()

        # hover and click to get rid of the floating list
        dummy = generate_page.find_element(By.XPATH, '//span[text()="Generate report"]')
        hover = ActionChains(driver).move_to_element(dummy)
        hover.click().perform()

    except Exception as err:
        logger.critical(err)
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)

    logger.info("Stores selected")

    # select suppliers
    try:
        btn_supplier = driver.find_element(
            By.XPATH, '//*[@id="supplierSelectBox"]/div/div/div/div/div/span/button'
        )
        driver.execute_script("arguments[0].click();", btn_supplier)
        sleep(1)

        supp_modal = driver.find_element(
            By.XPATH, '//div[@data-overlay-container="true"]/div/div'
        )

    except Exception as err:
        logger.critical(err)
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)

    try:
        selected_items = ["All"]
        is_over = False

        nr_suppliers = 0
        while not is_over:
            is_over = True

            # read the visible portion of the list
            supplier_list = generate_page.find_elements(
                By.XPATH, '//div[@id="supplier-select-menu"]/div/div/ul/li'
            )
            sleep(1)

            if len(supplier_list) == 0:
                break
            else:
                nr_suppliers += len(supplier_list)

            # if there are suppliers not selected yet, select and add them to the list
            for item in supplier_list:
                if item.text not in selected_items:
                    item.click()
                    selected_items.append(item.text)
                    is_over = False

            # scroll down to see if there are more suppliers in the list
            driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].offsetHeight;",
                supp_modal,
            )
            sleep(1)
        logger.info(f"There are {nr_suppliers} suppliers in the list")

        # hover and click to get rid of the floating list
        dummy = generate_page.find_element(By.XPATH, '//span[text()="Generate report"]')
        hover = ActionChains(driver).move_to_element(dummy)
        hover.click().perform()

    except Exception as err:
        logger.critical(str(err))
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)

    logger.info("Suppliers selected")

    # set the date of the report  ---- not developed yet
    try:
        date_element = generate_page.find_element(
            By.XPATH, '//input[contains(@id,"mui-") and @placeholder = "dd/mm/yyyy"]'
        )
    except Exceptions.NoSuchElementException:
        logger.critical("calendar date not found")
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": "Calendar date not found",
                "error_details": None
            }
        raise ScrapperException(reply)

    rpt_date = date_element.get_attribute("value")
    rpt_date = rpt_date[:10]

    today = datetime.now()
    today = today.strftime("%d-%m-%Y")
    today = today.replace("-", "/")

    if rpt_date != today:
        logger.error("Data raportului nu corespunde datei curente {rpt_date}")
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"Data raportului nu corespunde datei curente: {rpt_date}",
                "error_details": None
            }
        raise ScrapperException(reply)

    logger.info("Current date verified")

    # check download as zip
    try:
        generate_page.find_element(
            By.XPATH,
            "/html/body/div[4]/div[3]/div/form/div[1]/div/div[5]/label/span[1]/input",
        ).click()
    except Exception as err:
        logger.critical(err)
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)

    logger.info("Checked ZIP format")

    # wait until download complete
    nr_attempts = 10
    is_downloaded = False
    try:
        generate_page.find_element(
            By.XPATH, "/html/body/div[4]/div[3]/div/form/div[2]/button[2]"
        ).click()
    except Exception as err:
        logger.critical(str(err))
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)

    for i in range(nr_attempts):
        if os.path.exists("/src/date/Bulk PO.zip"):
            is_downloaded = True
            break
        else:
            sleep(3)

    if not is_downloaded:
        logger.critical("Timeout exceeded. Bulk PO file download failed")
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": "Bulk PO download timeout exceeded.",
                "error_details": None
            }
        raise ScrapperException(reply)

    logger.info("File downloaded")

    # cancel and quit
    try:
        generate_page.find_element(
            By.XPATH, "/html/body/div[4]/div[3]/div/form/div[2]/button[1]"
        ).click()

    except Exception as err:
        logger.warning(str(err))
        driver.quit()
        reply = {
                "function_name": "Scrapper",
                "error_message": f"general error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)

    driver.quit()
    
    # save output files to s3
    s3_client = boto3.client("s3", region_name="eu-north-1")
    try:
        s3_client.upload_file(
            "/tmp/Bulk PO.zip", 
            "bolt-projects", 
            "purchasing-orders/input/Bulk PO.zip")
    except Exception as err:
        reply = {
                "function_name": "Scrapper",
                "error_message": f"Bulk PO transfer to s3 error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)
    
    try:
        s3_client.upload_file(
            "/tmp/mov_data.csv", 
            "bolt-projects", 
            "purchasing-orders/input/mov_data.csv")
    except Exception as err:
        reply = {
                "function_name": "Scrapper",
                "error_message": f"Bulk PO transfer to s3 error: {str(err)}",
                "error_details": None
            }
        raise ScrapperException(reply)
    
    logger.info("procedure finalized and stopped successfully")

    return {
        "function_name": "Scrapper",
        "error_message": None,
        "error_details": None,
    }