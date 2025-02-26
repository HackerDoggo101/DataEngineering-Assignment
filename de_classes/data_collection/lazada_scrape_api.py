## Author: Wong Yee En
import requests
from bs4 import BeautifulSoup
import json
import csv

class LazadaScrapeWithAPI:
    def __init__(self, auth_credentials):
        """
        Initialize the LazadaScrapeWithAPI class with authentication credentials for the Oxylabs API.
        """
        self.auth_credentials = auth_credentials
        self.all_reviews = []

    def scrape_reviews(self, urls):
        """
        Scrape reviews from a list of Lazada product URLs using the Oxylabs API.
        
        Args:
        urls (list): List of Lazada product URLs to scrape.
        
        Returns:
        None
        """
        for url in urls:
            # Set up proxy with requests
            payload = {
                'source': 'universal',
                'url': url,
                'render': 'html',
                'user_agent_type': 'desktop',
                'context': [{'key': 'follow_redirects', 'value': True}],
            }

            response = requests.post(
                'https://realtime.oxylabs.io/v1/queries',
                auth=self.auth_credentials,
                json=payload,
            )

            html_content = response.json()['results'][0]['content']

            # Parse the HTML content with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')

            # Extract reviews for each URL
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
                    self.all_reviews.append((name_text, review_text, skuInfo_text, date_text, star_count))

    def export_to_json(self, file_name):
        """
        Export the scraped data to a JSON file.
        
        Args:
        file_name (str): The name of the JSON file to export the data.
        
        Returns:
        None
        """
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(self.all_reviews, f, ensure_ascii=False, indent=4)

    def export_to_csv(self, file_name):
        """
        Export the scraped data to a CSV file.
        
        Args:
        file_name (str): The name of the CSV file to export the data.
        
        Returns:
        None
        """
        with open(file_name, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Name', 'Review', 'SkuInfo', 'Date', 'StarCount'])
            for name, review, skuInfo, date, star_count in self.all_reviews:
                writer.writerow([name, review, skuInfo, date, star_count])

    def clear_reviews(self):
        """
        Clear the stored reviews.
        
        Returns:
        None
        """
        self.all_reviews = []
