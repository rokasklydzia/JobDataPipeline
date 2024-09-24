import pypetteer

browser = await launch(headless=False)
page = await browser.newPage()