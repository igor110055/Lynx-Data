import sqlite3

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from sockets.data import Database
import pandas as pd
import time


def grab_data():
    try:
        db.execute(sql)
    except sqlite3.OperationalError:
        print("No table named arby. Creating new table")
    table = driver.find_element(By.XPATH, "//div[@class='intra-table']")

    for row in table.find_elements(By.XPATH, "//div[@class='intra-table-row']"):
        pairs = []
        bids = []
        asks = []
        volumes = []
        date = row.find_element(By.XPATH, ".//div[@class='time']")
        for pair in row.find_elements(By.XPATH, ".//a[@class='token']"):
            pairs.append(pair.text)
        profit = row.find_element(By.XPATH, ".//div[@class='profit right']")
        for bid in row.find_elements(By.XPATH, ".//div[@class='bid']/div"):
            bids.append(bid.text.replace(',', ''))
        for ask in row.find_elements(By.XPATH, ".//div[@class='ask']/div"):
            asks.append(ask.text)
        for volume in row.find_elements(By.XPATH, ".//div[@class='volume']/div"):
            volumes.append(volume.text)
        latency = row.find_element(By.XPATH, ".//div[@class='delay']")
        for index, cur in enumerate(pairs):
            binance_pair = pairs[index].replace("/", "")
            if index == 0 or index + 1 % 3 == 0:
                purchased = (100 / float(bids[index])) * 0.925
            else:
                purchased = (purchased * float((bids[index]))) * 0.925
            res = {"time": date.text, "profit": profit.text, "pair": pairs[index], "api": binance_pair, "bid": bids[index], "ask": asks[index], "volume": volumes[index], "latency": latency.text, "purchased": purchased}
            df = pd.DataFrame([res])
            print(df)
            df.to_sql("arby",
                      con=db.connection,
                      if_exists='append')


db = Database("databases/Arby.db")
driver = webdriver.Chrome()
actions = ActionChains(driver)
driver.get("https://arby.trade/site/login")
email_field = driver.find_element(By.XPATH, "//input[@placeholder='Your email']")
pass_field = driver.find_element(By.XPATH, "//input[@placeholder='Password']")
submit_button = driver.find_element(By.XPATH, "//button[@type='submit']")
actions.click(email_field)
actions.send_keys('maazabdul@ymail.com')
actions.click(pass_field)
actions.send_keys('Dexdata')
actions.click(submit_button)
actions.perform()

wait = WebDriverWait(driver, 30)
wait.until(EC.url_to_be('https://arby.trade/interexchange'))
driver.get("https://arby.trade/pilot/intra-arbitrage")
wait.until(EC.visibility_of_element_located((By.XPATH, "//div[@class='intra-table-row']")))

sql = "DELETE FROM arby"

while True:
    try:
        print("Refreshing Table")
        grab_data()
        time.sleep(20)
    except KeyboardInterrupt:
        print("Closed Connection")
