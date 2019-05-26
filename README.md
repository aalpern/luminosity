# Luminosity

A library for accessing Adobe Lightroom catalogs.

Here's something you can do with the output -
[Lightroom Catalog Visualization with Go and d3 ](https://monster.partyhat.co/article/lightroom-catalog-visualization/).

Documentation is available on
[godoc.org](https://godoc.org/github.com/aalpern/luminosity).

## Origins

This library began as a very simple script to purge sidecar files from
disk. The bulk of my photography is travel photography, and for my
current setup I usually don't bring a laptop - I travel with an iPad
and a
[https://www.wd.com/products/portable-storage/my-passport-wireless-pro.html](WD
My Passport Wireless Pro) backup device, which is basically a portable
wifi NAS with SD card backup.

The Passport device can serve access to the downloaded photos - it's
slow, but it's sufficient for basic review, and for selecting
highlights to post while travelling, but it doesn't support RAW
files. Therefore, I shoot in RAW+JPG while travelling, and when I
return and back everything up to my
[Synology](https://monster.partyhat.co/article/photo-backup-evolution-2019-edition/)
I purge the sidecar files. 

Since every Lightroom catalog is actually a
[Sqlite](https://www.sqlite.org/) database file, running SQL queries
to extract the sidecar paths was trivial - that ensures that I'm not
deleting any images that were shot as JPG only.

Since then, the codebase has evolved significantly thanks to my
interests in [data
visualization](https://monster.partyhat.co/article/lightroom-catalog-visualization/).
