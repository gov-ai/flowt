echo '🚀 making minor changes to be able to push code and trigger workflow ...'
echo 'init scraping $(date)' > log.txt
git add log.txt
git commit -m"`date +'%Y-%m-%d %H:%M:%S'`"
git push origin workflow/release-scraped-data