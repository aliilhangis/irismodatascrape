#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ürün Scraper v4.0 - Temiz ve Basit"""

import requests
from bs4 import BeautifulSoup
import time
import re
from supabase import create_client
from datetime import datetime
from urllib.parse import urlparse
import hashlib

SUPABASE_URL = "https://zmmpuysxnwqngvlafolm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InptbXB1eXN4bndxbmd2bGFmb2xtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjkwNjA0MTAsImV4cCI6MjA4NDYzNjQxMH0.4Q7k-cDcaGhOurMlofG8lkd4ApPyYexxkMdXxH-lI0k"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Site konfigürasyonları
SITE_CONFIGS = {
    'technopluskibris.com': {
        'name': 'TECHNOPLUSKIBRIS',
        'price_selectors': ['.product-price span', '.product-price', 'span[class*="price"]'],
        'title_selectors': ['h1.product-name', 'h1', 'title'],
        'currency': 'TL'
    },
    'durmazz.com': {
        'name': 'DURMAZZ',
        'price_selectors': ['span[itemprop="price"]', '.oe_currency_value', 'span.oe_price'],
        'title_selectors': ['h1[itemprop="name"]', 'h1', 'title'],
        'currency': 'USD'
    },
    'irismostore.com': {
        'name': 'IRISMOSTORE',
        'price_selectors': [
            'div.price-usd',  # ← İŞTE BURASI! div class="price-usd"
            '.price-usd',
            'div[class*="price-usd"]',
            '.usd-price-line div',
            'h3'
        ],
        'title_selectors': ['h1', 'title'],
        'currency': 'USD'
    },
    'sharafstore.com': {
        'name': 'SHARAFSTORE',
        'price_selectors': ['span.price', '.product-price span', 'span[class*="price"]'],
        'title_selectors': ['h1.product-title', 'h1', 'title'],
        'currency': 'TL'
    }
}

def get_all_urls():
    """productofsitemapcrawl tablosundan TÜM URL'leri çek - PAGINATION ile"""
    print("\n📥 Tüm URL'ler çekiliyor (pagination)...")
    
    try:
        all_urls = []
        page_size = 1000
        offset = 0
        
        while True:
            print(f"  📄 Sayfa {offset//page_size + 1} çekiliyor...")
            
            response = supabase.table('productofsitemapcrawl')\
                .select('id, url, anawebsite')\
                .range(offset, offset + page_size - 1)\
                .execute()
            
            if not response.data or len(response.data) == 0:
                break
            
            all_urls.extend(response.data)
            print(f"     └─ {len(response.data)} kayıt eklendi (Toplam: {len(all_urls)})")
            
            # Eğer page_size'dan az gelirse, bitti demektir
            if len(response.data) < page_size:
                break
            
            offset += page_size
        
        if all_urls:
            print(f"\n  ✅ TOPLAM {len(all_urls)} URL çekildi")
            
            # Site bazında göster
            from collections import Counter
            sites = Counter([r.get('anawebsite', 'unknown') for r in all_urls])
            print(f"  📊 Site dağılımı:")
            for site, count in sorted(sites.items(), key=lambda x: x[1], reverse=True):
                print(f"     └─ {site}: {count} URL")
            
            return all_urls
        else:
            print("  ⚠️ Hiç URL yok!")
            return []
            
    except Exception as e:
        print(f"  ❌ Hata: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_site_config(url):
    """URL'den site config'ini bul - www'lu www'suz fark etmez"""
    domain = urlparse(url).netloc.lower()
    # www. varsa da yoksa da eşleşsin
    domain_clean = domain.replace('www.', '')
    
    for config_domain, config in SITE_CONFIGS.items():
        config_clean = config_domain.replace('www.', '')
        # Hem www'lu hem www'suz kontrol et
        if config_clean in domain or config_domain in domain:
            return config
    
    # Default
    return {
        'name': domain.upper().replace('WWW.', ''),
        'price_selectors': ['.price', 'span.price'],
        'title_selectors': ['h1', 'title'],
        'currency': 'TL'
    }

def extract_price(soup, selectors):
    """Fiyatı çıkar - basit ve etkili"""
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if not element:
                continue
            
            text = element.get_text(strip=True)
            if not text:
                continue
            
            # Sadece rakamları al
            numbers = re.sub(r'[^\d.]', '', text)
            
            if numbers:
                price = float(numbers)
                if price > 0:
                    return price
        except:
            continue
    
    return None

def extract_title(soup, selectors):
    """Başlığı çıkar"""
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if element.name == 'title':
                    title = title.split('|')[0].split('-')[0].strip()
                if title and len(title) > 3:
                    return title
        except:
            continue
    
    return "Bilinmiyor"

def generate_sku(url, site_name):
    """SKU oluştur"""
    url_part = url.rstrip('/').split('/')[-1][:30]
    site_prefix = site_name[:3].upper()
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    return f"{site_prefix}-{url_part}-{url_hash}"

def scrape_url(url, config):
    """Tek bir URL'yi scrape et"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        title = extract_title(soup, config['title_selectors'])
        price = extract_price(soup, config['price_selectors'])
        currency = config['currency']
        
        return {
            'title': title,
            'price': price,
            'currency': currency,
            'url': url,
            'site': config['name']
        }
    
    except Exception as e:
        print(f"      ✗ Hata: {str(e)[:50]}")
        return None

def save_to_db(product, site_name):
    """products tablosuna kaydet"""
    try:
        sku = generate_sku(product['url'], site_name)
        
        # Fiyat değişimi kontrolü
        existing = supabase.table('products').select('price').eq('sku', sku).execute()
        
        price_change = None
        previous_price = None
        price_changed_at = None
        
        if existing.data:
            old_price = existing.data[0].get('price')
            if old_price and product['price']:
                if float(old_price) != float(product['price']):
                    previous_price = old_price
                    price_change = float(product['price']) - float(old_price)
                    price_changed_at = datetime.now().isoformat()
                    
                    if price_change > 0:
                        print(f"      📈 Fiyat arttı: {old_price} → {product['price']}")
                    else:
                        print(f"      📉 Fiyat düştü: {old_price} → {product['price']}")
        
        # Kaydet
        data = {
            'sku': sku,
            'name': product['title'],
            'price': product['price'],
            'previous_price': previous_price,
            'price_change': price_change,
            'price_changed_at': price_changed_at,
            'stock_status': 'in_stock' if product['price'] else 'unknown',
            'url': product['url'],
            'product_name': product['title'],
            'product_url': product['url'],
            'stock_data': {
                'site': site_name,
                'currency': product['currency'],
                'scraped_at': datetime.now().isoformat()
            },
            'scraped_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        supabase.table('products').upsert(data, on_conflict='sku').execute()
        return True
        
    except Exception as e:
        print(f"      ❌ DB hatası: {str(e)[:50]}")
        return False

def main():
    print("\n" + "="*70)
    print("🚀 ÜRÜN SCRAPER v4.0")
    print("="*70)
    
    # TÜM URL'leri çek
    urls = get_all_urls()
    
    if not urls:
        print("\n⚠️ Hiç URL bulunamadı!")
        return
    
    print(f"\n📊 {len(urls)} URL scrape edilecek")
    print("─"*70)
    
    stats = {'success': 0, 'failed': 0, 'no_price': 0}
    
    for i, record in enumerate(urls, 1):
        url = record['url']
        site_name = record.get('anawebsite', '')
        
        # Config al
        config = get_site_config(url)
        
        print(f"[{i}/{len(urls)}] {config['name'][:20]:20s}", end=" ")
        
        # Scrape et
        product = scrape_url(url, config)
        
        if product:
            if product['price']:
                # Fiyat var - kaydet
                if save_to_db(product, config['name']):
                    print(f"💾 {product['title'][:30]:30s} - {product['price']} {product['currency']}")
                    stats['success'] += 1
                else:
                    print(f"⚠️ Kayıt başarısız")
                    stats['failed'] += 1
            else:
                # Fiyat yok
                print(f"❌ {product['title'][:30]:30s} - Fiyat yok")
                stats['no_price'] += 1
        else:
            # Scrape başarısız
            print(f"✗ Scrape başarısız")
            stats['failed'] += 1
        
        # Rate limiting
        if i % 10 == 0:
            time.sleep(1)
        else:
            time.sleep(0.3)
    
    # Özet
    print("\n" + "="*70)
    print("📊 ÖZET")
    print("="*70)
    print(f"Toplam URL: {len(urls)}")
    print(f"✅ Başarılı: {stats['success']}")
    print(f"❌ Fiyat yok: {stats['no_price']}")
    print(f"✗ Hata: {stats['failed']}")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()
