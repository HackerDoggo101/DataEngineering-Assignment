## Author: Wong Yee En
import time
from selenium import webdriver
from bs4 import BeautifulSoup
import json
import csv
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class LazadaScrapeWithoutAPI:
    def __init__(self, url, max_pages=5):
        """
        Initialize the LazadaScrapeWithoutAPI class.
        
        Args:
        url (str): The URL of the Lazada product page to scrape.
        max_pages (int): The maximum number of pages to scrape reviews from.
        """
        self.url = url
        self.max_pages = max_pages
        self.reviews = []
        self.driver = None

    def initialize_driver(self):
        """
        Initialize the Selenium WebDriver with Chrome options.
        """
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        self.driver = webdriver.Chrome(options=options)
        self.driver.get(self.url)
        body = self.driver.find_element(By.TAG_NAME, 'body')
        for _ in range(10):  # Adjust the range for more/less increments
            body.send_keys(Keys.ARROW_DOWN)  # Scroll a small amount down
            body.send_keys(Keys.ARROW_DOWN) 
            body.send_keys(Keys.ARROW_DOWN) 
            time.sleep(2)  # Adjust sleep duration for slower or faster scrolling

    def scrape_reviews(self):
        """
        Scrape the reviews from the Lazada product page.
        """
        self.initialize_driver()

        for i in range(0, self.max_pages):
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')

            nameContainers = soup.findAll('div', class_='middle')
            containers = soup.findAll('div', class_='item-content')
            dateContainers = soup.findAll('div', class_='top')

            for nameContainer, container, dateContainer in zip(nameContainers, containers, dateContainers):
                name = nameContainer.find('span')
                review = container.find('div', class_='content')
                skuInfo = container.find('div', class_='skuInfo')
                date = dateContainer.find('span', class_='title right')
                stars = dateContainer.find('div', class_='container-star starCtn left').find_all('img', class_='star', src='//img.lazcdn.com/g/tps/tfs/TB19ZvEgfDH8KJjy1XcXXcpdXXa-64-64.png')
                star_count = len(stars)

                if name and review and skuInfo and date and stars:
                    name_text = name.get_text(strip=True)
                    review_text = review.get_text(strip=True)
                    skuInfo_text = skuInfo.get_text(strip=True)
                    date_text = date.get_text(strip=True)
                    self.reviews.append((name_text, review_text, skuInfo_text, date_text, star_count))
                    print(name_text, review_text, skuInfo_text, date_text, star_count)

            time.sleep(2)

            # Try to click the next button to go to the next page
            try:
                next_button = self.driver.find_element(By.CSS_SELECTOR, "button[class='next-btn next-btn-normal next-btn-medium next-pagination-item next']")
                next_button.click()
            except:
                print("No more pages.")
                break

            time.sleep(3)

        self.driver.quit()

    def export_to_json(self, file_name):
        """
        Export the scraped reviews to a JSON file.
        
        Args:
        file_name (str): The name of the JSON file to export the data.
        """
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(self.reviews, f, ensure_ascii=False, indent=4)

    def export_to_csv(self, file_name):
        """
        Export the scraped reviews to a CSV file.
        
        Args:
        file_name (str): The name of the CSV file to export the data.
        """
        with open(file_name, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Name', 'Review', 'SkuInfo', 'Date', 'StarCount'])
            for name, review, skuInfo, date, star_count in self.reviews:
                writer.writerow([name, review, skuInfo, date, star_count])

    def clear_reviews(self):
        """
        Clear the stored reviews.
        """
        self.reviews = []
