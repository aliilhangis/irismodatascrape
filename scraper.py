#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
İyileştirilmiş Ürün Scraper v2.1
- Sitemap index support
- PostgreSQL entegrasyonu
- Her site için özel pattern'ler  
"""

import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin
import time
import re
from xml.etree import ElementTree as ET
import psycopg2
from psycopg2.extras import Json
import os
from datetime import datetime
import hashlib
from dotenv import load_dotenv

# .env dosyasını yükle
load_dotenv()

# PostgreSQL bağlantı ayarları
# ÖNCE .env'den oku, yoksa bu değerleri kullan
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'zmmpuysxnwqngvlafolm.supabase.co'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'irisfiyattakip'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'Ali.1995Ft2828'),
}

# TEST MODE - Sadece ilk N ürünü scrape et (0 = tümü)
TEST_LIMIT = 10  # Test için 10 ürün, production'da 0 yapın

# Site konfigürasyonları
SITE_CONFIGS = {
    'technopluskibris.com': {
        'name': 'TECHNOPLUSKIBRIS',
        'sitemap_url': 'https://technopluskibris.com/sitemap.xml',
        'sitemap_type': 'index',
        'product_sitemap_pattern': r'products_\d+\.xml',
        'product_url_pattern': r'/prd-',
        'selectors': {
            'title': [
                'h1.product-name',
                'h1.product-title',
                '.product-detail h1',
                'h1',
                'title'
            ],
            'price': [
                '.product-price span',
                '.product-price',
                'span[class*="price"]',
                'div[class*="price"]',
                'meta[property="product:price:amount"]'
            ],
            'currency': 'TL'
        }
    },
    'durmazz.com': {
        'name': 'DURMAZZ',
        'sitemap_url': 'https://www.durmazz.com/sitemap.xml',
        'sitemap_type': 'index',
        'product_sitemap_pattern': r'shop-sitemap\.xml',
        'product_url_pattern': r'/shop/(product-\d+|[a-z0-9\-]+\-\d+)',
        'exclude_patterns': [r'/cart', r'/wishlist', r'/category', r'/checkout', r'/page/', r'/compare'],
        'selectors': {
            'title': [
                'h1[itemprop="name"]',
                '.product-title h1',
                'h1.product-name',
                '.oe_product h1',
                'h1'
            ],
            'price': [
                'span[itemprop="price"]',
                '.oe_currency_value',
                'span.oe_price',
                'div[class*="price"] span',
                'meta[property="product:price:amount"]'
            ],
            'currency': 'USD'
        }
    }
}

def generate_sku(url, site_name):
    """URL'den benzersiz SKU oluştur"""
    # URL'nin son kısmını al
    url_part = url.rstrip('/').split('/')[-1]
    # Site prefix ekle
    site_prefix = site_name[:3].upper()
    # Hash oluştur (kısa tutmak için ilk 8 karakter)
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    return f"{site_prefix}-{url_part[:30]}-{url_hash}"

def get_db_connection():
    """PostgreSQL bağlantısı oluştur"""
    try:
        print(f"  🔌 Bağlantı deneniyor: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"  ✅ Bağlantı başarılı!")
        return conn
    except Exception as e:
        print(f"  ❌ Veritabanı bağlantı hatası: {e}")
        return None

def init_database():
    """Veritabanı bağlantısını test et"""
    print("\n🔍 Veritabanı bağlantısı test ediliyor...")
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM products;")
            count = cursor.fetchone()[0]
            print(f"✅ Veritabanında şu anda {count} ürün var")
            cursor.close()
        except Exception as e:
            print(f"⚠️ Tablo sorgu hatası: {e}")
            print("💡 'products' tablosunun oluşturulduğundan emin olun!")
        finally:
            conn.close()
        return True
    else:
        print("⚠️ Veritabanı bağlantısı kurulamadı - sadece JSON'a kaydedilecek")
        return False

def save_product_to_db(product, site_name):
    """Ürünü veritabanına kaydet"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # SKU oluştur
        sku = generate_sku(product['url'], site_name)
        
        # Stock status belirle
        stock_status = 'in_stock' if product['price'] is not None else 'unknown'
        
        # Stock data oluştur
        stock_data = {
            'site': site_name,
            'currency': product.get('currency'),
            'last_seen_price': product.get('price'),
            'scraped_at': datetime.now().isoformat()
        }
        
        # Insert veya update
        query = """
        INSERT INTO products 
            (sku, name, price, stock_status, url, product_name, product_url, 
             stock_data, scraped_at, updated_at)
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sku) 
        DO UPDATE SET
            name = EXCLUDED.name,
            price = EXCLUDED.price,
            stock_status = EXCLUDED.stock_status,
            product_name = EXCLUDED.product_name,
            stock_data = EXCLUDED.stock_data,
            scraped_at = EXCLUDED.scraped_at,
            updated_at = EXCLUDED.updated_at
        """
        
        now = datetime.now()
        
        cursor.execute(query, (
            sku,
            product['title'],
            product['price'],
            stock_status,
            product['url'],
            product['title'],
            product['url'],
            Json(stock_data),
            now,
            now
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"      ❌ DB kayıt hatası: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        return False

def get_sitemap_urls(sitemap_url):
    """Sitemap'ten URL'leri çeker"""
    try:
        print(f"  Fetching: {sitemap_url}")
        response = requests.get(sitemap_url, timeout=15)
        response.raise_for_status()
        
        urls = []
        root = ET.fromstring(response.content)
        
        # <loc> taglerini bul
        for loc in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
            urls.append(loc.text.strip())
        
        # Namespace olmadan da dene
        if not urls:
            for loc in root.findall('.//loc'):
                urls.append(loc.text.strip())
        
        return urls
        
    except Exception as e:
        print(f"  ✗ Sitemap error: {e}")
        return []

def get_product_sitemaps(config):
    """Sitemap index'ten ürün sitemap'lerini bulur"""
    sitemap_url = config['sitemap_url']
    sitemap_type = config.get('sitemap_type', 'direct')
    
    if sitemap_type == 'direct':
        return [sitemap_url]
    
    print(f"📑 Sitemap index okunuyor...")
    all_sitemaps = get_sitemap_urls(sitemap_url)
    
    if not all_sitemaps:
        return []
    
    product_sitemap_pattern = config.get('product_sitemap_pattern', '')
    product_sitemaps = []
    
    for sitemap in all_sitemaps:
        if product_sitemap_pattern and re.search(product_sitemap_pattern, sitemap):
            product_sitemaps.append(sitemap)
    
    print(f"  ✓ {len(product_sitemaps)} ürün sitemap bulundu")
    return product_sitemaps

def filter_product_urls(urls, config):
    """URL'leri filtrele"""
    product_urls = []
    product_pattern = config.get('product_url_pattern', '')
    exclude_patterns = config.get('exclude_patterns', [])
    
    for url in urls:
        if product_pattern and re.search(product_pattern, url):
            is_excluded = False
            for exclude_pattern in exclude_patterns:
                if re.search(exclude_pattern, url):
                    is_excluded = True
                    break
            
            if not is_excluded:
                product_urls.append(url)
    
    return product_urls

def extract_price(soup, selectors):
    """Fiyat çıkar"""
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    price_text = element.get('content', '')
                else:
                    price_text = element.get_text(strip=True)
                
                price_text = price_text.replace(',', '').replace(' ', '')
                price_match = re.search(r'(\d+\.?\d*)', price_text)
                
                if price_match:
                    try:
                        price = float(price_match.group(1))
                        if price > 0:
                            return price
                    except:
                        continue
        except:
            continue
    
    return None

def extract_title(soup, selectors):
    """Başlık çıkar"""
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if element.name == 'title':
                    title = re.split(r'\s*[|\-]\s*', title)[0]
                
                if title and len(title) > 3:
                    return title
        except:
            continue
    
    return "Bilinmiyor"

def scrape_product(url, config, db_enabled=False):
    """Ürün scrape et"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        title = extract_title(soup, config['selectors']['title'])
        price = extract_price(soup, config['selectors']['price'])
        currency = config['selectors']['currency']
        
        product_data = {
            'title': title,
            'price': price,
            'currency': currency,
            'url': url
        }
        
        # Veritabanına kaydet
        if db_enabled:
            db_success = save_product_to_db(product_data, config['name'])
            db_icon = "💾" if db_success else "⚠️"
        else:
            db_icon = "📝"
        
        # Log
        if price is not None:
            print(f"    {db_icon} {title[:45]}... - {price} {currency}")
        else:
            print(f"    {db_icon} {title[:45]}... - Fiyat yok")
        
        return product_data
        
    except Exception as e:
        print(f"    ✗ Error: {str(e)[:50]}")
        return None

def scrape_site(config, db_enabled=False):
    """Site scrape et"""
    print(f"\n{'='*70}")
    print(f"🏪 SİTE: {config['name']}")
    print(f"{'='*70}")
    
    products = []
    
    product_sitemaps = get_product_sitemaps(config)
    
    if not product_sitemaps:
        print("✗ Ürün sitemap bulunamadı")
        return products
    
    all_product_urls = []
    for sitemap_url in product_sitemaps:
        print(f"\n📄 Sitemap: {sitemap_url.split('/')[-1]}")
        urls = get_sitemap_urls(sitemap_url)
        
        if urls:
            product_urls = filter_product_urls(urls, config)
            all_product_urls.extend(product_urls)
            print(f"  ✓ {len(product_urls)} ürün URL'si bulundu")
    
    all_product_urls = list(set(all_product_urls))
    
    # TEST LIMIT uygula
    if TEST_LIMIT > 0:
        print(f"\n⚠️ TEST MODU: Sadece ilk {TEST_LIMIT} ürün işlenecek")
        all_product_urls = all_product_urls[:TEST_LIMIT]
    
    if not all_product_urls:
        print("\n✗ Hiç ürün URL'si bulunamadı")
        return products
    
    print(f"\n📊 Toplam: {len(all_product_urls)} benzersiz ürün URL'si")
    print(f"\n🔍 İlk 3 URL örneği:")
    for url in all_product_urls[:3]:
        print(f"  • {url}")
    
    print(f"\n⚙️ Ürünler scrape ediliyor...")
    print(f"{'─'*70}")
    
    for i, url in enumerate(all_product_urls, 1):
        print(f"  [{i}/{len(all_product_urls)}]", end=" ")
        
        product = scrape_product(url, config, db_enabled)
        
        if product:
            products.append(product)
        
        if i % 10 == 0:
            time.sleep(1)
        else:
            time.sleep(0.3)
    
    print(f"{'─'*70}")
    print(f"✅ {len(products)} ürün başarıyla scrape edildi")
    
    return products

def main():
    """Ana fonksiyon"""
    print(f"\n{'='*70}")
    print("🚀 ÜRÜN SCRAPER BAŞLATILIYOR")
    print(f"{'='*70}")
    
    # Veritabanı kontrolü
    db_enabled = init_database()
    
    if db_enabled:
        print("💾 Veriler hem JSON hem PostgreSQL'e kaydedilecek")
    else:
        print("📝 Veriler sadece JSON'a kaydedilecek")
    
    all_products = []
    stats = {}
    
    for domain, config in SITE_CONFIGS.items():
        products = scrape_site(config, db_enabled)
        
        site_name = config['name']
        stats[site_name] = {
            'total': len(products),
            'with_price': len([p for p in products if p['price'] is not None]),
            'without_price': len([p for p in products if p['price'] is None])
        }
        
        for product in products:
            product['site'] = site_name
        
        all_products.extend(products)
    
    # JSON'a kaydet
    output_file = 'products.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_products, f, ensure_ascii=False, indent=2)
    
    # Özet
    print(f"\n{'='*70}")
    print("📊 ÖZET İSTATİSTİKLER")
    print(f"{'='*70}")
    print(f"Toplam Ürün: {len(all_products)}")
    
    for site_name, site_stats in stats.items():
        print(f"\n{site_name}:")
        print(f"  • Toplam: {site_stats['total']}")
        print(f"  • Fiyatlı: {site_stats['with_price']}")
        print(f"  • Fiyatsız: {site_stats['without_price']}")
    
    print(f"\n✅ JSON: '{output_file}' dosyasına kaydedildi")
    if db_enabled:
        print(f"✅ PostgreSQL: Tüm ürünler veritabanına kaydedildi")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
