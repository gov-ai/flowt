echo "setting user config"
git config --global user.email "rakeshark22@gmail.com"
git config --global user.name "Asapanna Rakesh"
echo '🚀 making minor changes to be able to push code and trigger scraping workflow ...'
echo "init scraping `date +'%Y-%m-%d %H:%M:%S'`" > log.txt
git checkout workflow/release-scraped-data
git add log.txt
git commit -m"trigger scraper `date +'%Y-%m-%d %H:%M:%S'`"
git push origin workflow/release-scraped-data