Hello,

In summary:

We are going to get links to EVERY filing for a given quarter, via bulk download, filter out only 13Fs and NPORTs, and then visit those filings and scrape for WOLF securities thru cusip.

Otherwise using the API in a more intuitive way that doesnt require manually downloading, means we have to sift through every filing for every company. Even doing it programmatically, black rock alone would require like 33k get requests, and i don't want to sit around for a week while i try to not get auto kicked by the SEC webmaster for scraping too quickly.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
INSTRUCTIONS:

First refer to the png file, showing you where you can find bulk sec data. We want the idx file containing all the filings, indexed by form type, so we can filter out all the noise a little faster. Save this as a txt file. Example in this folder as form.txt.

Run parse_bulk_edgar on this txt file. This will yield you "form_to_name_index.csv". Take a look at the file, it contains much nonsense. run "filter13F.py" on that file, yielding "filtered_form_index.csv" Much better. You should split this file into 2 files containing 13Fs and NPORTs respectively if I haven't implemented automatically doing so.

run scrape_13F.py on the 13Fs csv, run scrape_NPORT.py on the NPORTs. This is going to take a long time. We have to throttle our requests to the SEC so we dont get cut off by the bartender so to speak. Definitely going to take several hours to scrape all 13Fs and NPORTs for a given quarter.

In any case, you should have ALL positions for WOLF for a given quarter! Charting comes next. I would suggest naming these files manually to something helpful, most likely containing the effective filing date e.g. "sep_2024" or something like that. Repeat this process for however many quarters of data you want.
