import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime
from urllib.parse import urljoin
import re

class EcommerceScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.results = []
        
    def get_page(self, url, retries=3):
        """Sayfa içeriğini çek, hata durumunda tekrar dene"""
        for i in range(retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                return response
            except Exception as e:
                print(f"Hata ({i+1}/{retries}): {url} - {str(e)}")
                if i < retries - 1:
                    time.sleep(2)
        return None
    
    def extract_price_technopluskibris(self, soup):
        """Technopluskibris fiyat parse"""
        try:
            # Ürün Fiyatı bölümünü bul
            price_div = soup.find('div', string=re.compile('Ürün Fiyatı'))
            if price_div:
                # Bir sonraki kardeş elementi al (fiyatı içeren)
                price_text = price_div.find_next_sibling()
                if price_text:
                    # Sadece sayıları ve noktayı al
                    price = re.search(r'[\d.,]+', price_text.get_text())
                    if price:
                        return float(price.group().replace('.', '').replace(',', '.'))
        except Exception as e:
            print(f"Fiyat parse hatası: {e}")
        return None
    
    def extract_price_durmazz(self, soup):
        """Durmazz fiyat parse"""
        try:
            # Odoo e-ticaret yapısı için
            price_elem = soup.find('span', class_='oe_currency_value')
            if price_elem:
                price_text = price_elem.get_text().strip()
                price = re.search(r'[\d.,]+', price_text)
                if price:
                    return float(price.group().replace(',', ''))
        except Exception as e:
            print(f"Fiyat parse hatası: {e}")
        return None
    
    def scrape_product_technopluskibris(self, url):
        """Technopluskibris ürün bilgilerini çek"""
        print(f"Scraping: {url}")
        response = self.get_page(url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Ürün adı
        title = soup.find('h1')
        product_name = title.get_text().strip() if title else "Bilinmiyor"
        
        # Fiyat
        price = self.extract_price_technopluskibris(soup)
        
        # Stok durumu
        stock_elem = soup.find('div', string=re.compile('Stok Miktarı'))
        in_stock = False
        if stock_elem:
            stock_text = stock_elem.find_next_sibling()
            if stock_text and 'Stokta Var' in stock_text.get_text():
                in_stock = True
        
        return {
            'site': 'technopluskibris',
            'url': url,
            'name': product_name,
            'price': price,
            'currency': 'TL',
            'in_stock': in_stock,
            'scraped_at': datetime.now().isoformat()
        }
    
    def scrape_product_durmazz(self, url):
        """Durmazz ürün bilgilerini çek"""
        print(f"Scraping: {url}")
        response = self.get_page(url)
        if not response:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Ürün adı
        title = soup.find('h1', itemprop='name')
        product_name = title.get_text().strip() if title else "Bilinmiyor"
        
        # Fiyat
        price = self.extract_price_durmazz(soup)
        
        # Stok durumu
        in_stock = soup.find('button', string=re.compile('Add to Cart')) is not None
        
        return {
            'site': 'durmazz',
            'url': url,
            'name': product_name,
            'price': price,
            'currency': 'USD',
            'in_stock': in_stock,
            'scraped_at': datetime.now().isoformat()
        }
    
    def get_category_products_technopluskibris(self, category_url, max_products=10):
        """Technopluskibris kategori sayfasından ürün linklerini topla"""
        print(f"\nKategori taranıyor: {category_url}")
        response = self.get_page(category_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        product_links = []
        
        # Ürün linklerini bul (prd- ile başlayan linkler)
        links = soup.find_all('a', href=re.compile(r'/prd-'))
        
        for link in links[:max_products]:
            href = link.get('href')
            if href:
                full_url = urljoin('https://technopluskibris.com', href)
                if full_url not in product_links:
                    product_links.append(full_url)
        
        return product_links
    
    def get_category_products_durmazz(self, category_url, max_products=10):
        """Durmazz kategori sayfasından ürün linklerini topla"""
        print(f"\nKategori taranıyor: {category_url}")
        response = self.get_page(category_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        product_links = []
        
        # Ürün linklerini bul
        links = soup.find_all('a', href=re.compile(r'/shop/'))
        
        for link in links[:max_products]:
            href = link.get('href')
            if href and '/shop/' in href and '/category/' not in href:
                full_url = urljoin('https://www.durmazz.com', href)
                if full_url not in product_links:
                    product_links.append(full_url)
        
        return product_links
    
    def run(self, sites_config):
        """Ana scraping fonksiyonu"""
        print("="*60)
        print("E-TİCARET SCRAPER BAŞLATILIYOR")
        print("="*60)
        
        for site_config in sites_config:
            site_name = site_config['name']
            categories = site_config['categories']
            max_per_category = site_config.get('max_products', 5)
            
            print(f"\n{'='*60}")
            print(f"SİTE: {site_name.upper()}")
            print(f"{'='*60}")
            
            for category in categories:
                # Kategori linklerini al
                if site_name == 'technopluskibris':
                    product_urls = self.get_category_products_technopluskibris(
                        category, max_per_category
                    )
                    scrape_func = self.scrape_product_technopluskibris
                else:  # durmazz
                    product_urls = self.get_category_products_durmazz(
                        category, max_per_category
                    )
                    scrape_func = self.scrape_product_durmazz
                
                print(f"\n{len(product_urls)} ürün bulundu")
                
                # Her ürünü scrape et
                for url in product_urls:
                    product_data = scrape_func(url)
                    if product_data:
                        self.results.append(product_data)
                        print(f"✓ {product_data['name'][:50]}... - {product_data['price']} {product_data['currency']}")
                    
                    # Rate limiting
                    time.sleep(1)
        
        return self.results
    
    def save_results(self, filename='scraping_results.json'):
        """Sonuçları JSON dosyasına kaydet"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        print(f"\n{'='*60}")
        print(f"✓ {len(self.results)} ürün {filename} dosyasına kaydedildi")
        print(f"{'='*60}")
    
    def print_summary(self):
        """Özet istatistikler"""
        print(f"\n{'='*60}")
        print("ÖZET İSTATİSTİKLER")
        print(f"{'='*60}")
        
        total = len(self.results)
        techno_count = sum(1 for r in self.results if r['site'] == 'technopluskibris')
        durmazz_count = sum(1 for r in self.results if r['site'] == 'durmazz')
        
        print(f"Toplam Ürün: {total}")
        print(f"Technopluskibris: {techno_count}")
        print(f"Durmazz: {durmazz_count}")
        
        # Fiyat ortalaması
        prices = [r['price'] for r in self.results if r['price']]
        if prices:
            avg_price = sum(prices) / len(prices)
            print(f"\nOrtalama Fiyat: {avg_price:.2f}")
            print(f"En Düşük: {min(prices):.2f}")
            print(f"En Yüksek: {max(prices):.2f}")


# KULLANIM ÖRNEĞİ
if __name__ == "__main__":
    scraper = EcommerceScraper()
    
    # Hangi siteleri ve kategorileri tarayacağımızı belirle
    sites_to_scrape = [
        {
            'name': 'technopluskibris',
            'categories': [
                'https://technopluskibris.com/ev-aletleri-ve-yasam/mutfak-gerecleri/kahve-makineleri',
                'https://technopluskibris.com/telefon/telefonlar'
            ],
            'max_products': 5  # Her kategoriden kaç ürün
        },
        {
            'name': 'durmazz',
            'categories': [
                'https://www.durmazz.com/shop/category/apple-iphone-accessories-iphone-s-2138',
                'https://www.durmazz.com/shop/category/computer-technology-laptops-2245'
            ],
            'max_products': 5
        }
    ]
    
    # Scraping'i başlat
    results = scraper.run(sites_to_scrape)
    
    # Sonuçları kaydet
    scraper.save_results('products.json')
    
    # Özet göster
    scraper.print_summary()
