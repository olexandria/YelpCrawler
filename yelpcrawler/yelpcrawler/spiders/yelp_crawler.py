import scrapy
from scrapy import Request

category = "Contractors"
location = "San Francisco, CA"


def start_url(category_name, location_name):
    new_location = [i.strip().replace(" ", "+") for i in location_name.split(",")]
    start_url = f"https://www.yelp.com/search?find_desc={category_name}&find_loc={new_location[0]}%2C+{new_location[1]}"
    return start_url


class YelpCrawlingSpider(scrapy.Spider):
    name = "yelpcrawler"
    allowed_domains = ["yelp.com"]

    start_urls = [start_url(category_name=category, location_name=location)]

    def parse(self, response):
        businesses = response.xpath("//div[contains(@class, 'container__09f24__mpR8_ hoverable')]")

        for business in businesses:
            url = business.xpath(".//a[contains(@class, 'css-1um3nx')]//@href").get()
            business_url = response.urljoin(url)

            yield Request(business_url, callback=self.parse_details, meta={'business_yelp_url':business_url})

        next_page = response.xpath("//div[contains(@class, 'navigation-button-container')]//a[contains(@class, 'next-link')]//@href").get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)


    def parse_details(self, response):
        business_name = response.css("h1.css-1se8maq::text").get()
        business_rating = response.xpath("//div[contains(@class, 'five-stars__09f24__mBKym')]/@aria-label").get()
        number_of_reviews = response.css("a.css-1m051bw::text").get()
        business_yelp_url = response.meta.get('business_yelp_url')
        business_website = response.css(".css-1vhakgw .css-1p9ibgf > a[href*='biz_redir']::text").extract()

        reviews = []
        for review in response.css(".review__09f24__oHr9V")[:5]:
            review_name = review.css(".review__09f24__oHr9V .css-1m051bw::text").get(),
            review_location = review.css(".css-qgunke::text").get(),
            review_date = review.css(".css-chan6m::text").get(),

            reviews.append({
                "name": review_name,
                "location": review_location,
                "date": review_date,
            })

        dict = {
            "business_name": business_name,
            "business_rating": business_rating,
            "number_of_reviews": number_of_reviews,
            "business_yelp_url": business_yelp_url,
            "business_website": business_website,
            "reviews": reviews
        }

        yield dict
