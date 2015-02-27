##TO DO
# Intify writes into db: donelist -
## practiceResults


#This script scrapes results data by year from the formula1.com/results website
#The script is a *live* document and is subject to change/improvement

#Non-standard dependencies:
## These libraries may need installing before first use of this script:
## lxml, scraperwiki
#From the console:
## pip install lxml
## pip install scraperwiki
# Note: you may need to install the libraries as root, in which case (Mac/Linux):
# sudo pip install lxml
# sudo pip install scraperwiki

#Usage:
#In the terminal:
## cd to the directory containing this file
## Edit this file to define the appropriate settings
## Run: python f1-megascrapercode.py

#SETTINGS

#years: list containing the years you want to scrape data for
years=['2014']
years=range(2014,1949,-1)
#latest:
## 0 - scrape all races
## 1 - only scrape data from the most recent race
## N - (for integer N>0),  scrape data from the most recent N races
latest=0

#scraping:
# A cryptic setting that determines which sessions to scrape data for
## 1 scrapeset=["P1","P2","P3"]
## 2:scrapeset=["P1","P2"]
## 3:scrapeset=["P3"]
## 4:scrapeset=["P3","Q"]
## 5:scrapeset=["Q"]
## 6:scrapeset=["R"]
## 7:scrapeset=["P1","P2","P3","Q"]
## 8:scrapeset=["P1","P2","P3","Q","R"]
## 9:scrapeset=["P","P1","P2","P3","Q","R"]
scraping=9



##TO DO
## Check all tables are parsed correctly
## Test behaviour for "older" years
## Implement proper command line interface
## Define a better scraping/scrapeset parameter
## Use proper primary keys in database tables ??DONE??

#ERRORS/BUGS
##IF YOU FIND ANY ERRORS IN THE WAY ANY OF THE DATA TABLE HAVE BEEN PARSED,
##PLEASE POST AN ISSUE TO https://github.com/psychemedia/wranglingf1datawithr/issues 


#UP TO 2005 incl, there are results for 4 practice sessions and 2 qualis

#--------- YOU SHOULD NOT NEED TO GO BELOW HERE

#nodrop:
## 0 - drop all tables from current database first 
## 1 - retain tables in database - add rows on top of previous rows;
## Note that unique data keys are not currently used (?still true?) so retaining tables may duplicate rows 
nodrop=0


#Standard libraries
import urllib, csv

#Additional Libraries
#These may need installing before first use
#From the command line:
#pip install lxml
#pip install scraperwiki
import lxml.html, scraperwiki

def flatten(el):
    """ Helper function to flatten HTML tags by selecting text contained within them """
    result = [ (el.text or "") ]
    for sel in el:
        result.append(flatten(sel))
        result.append(sel.tail or "")
    return "".join(result).strip()


def dropper(table):
    """ Helper function to drop a table """
    if nodrop==1: return
    print "dropping",table
    if table!='':
        try: scraperwiki.sqlite.execute('drop table "'+table+'"')
        except: pass

def formatTime(t):
    """ Format time in preferred way: m.sss """
    return float("%.3f" % t)
    

def getTime(ts):
    """
        Accept times in the form of hh:mm:ss.ss or mm:ss.ss
        Return the equivalent number of seconds
    """
    tm=float('nan')
    if ts.find(':')>-1:
        t=ts.strip()
        t=ts.split(':')
        if len(t)==3:
            tm=60*int(t[0])+60*int(t[1])+float(t[2])
        elif len(t)==2:
            tm=60*int(t[0])+float(t[1])
        tm=formatTime(tm)
    else:
        try:
            tm=float(ts)
            tm=formatTime(tm)
        except:tm=float('nan')
    return tm

def tryint(s):
	""" Try to cast a possible string as an integer""" 
	try:
		return int(s)
	except ValueError:
		return s

def qSpeedScraper(sessions,tn,year):
    """ Scrape speed_trap.html results """
    dropper(tn)
    bigdata=[]
    for quali in sessions:
        url=quali[0]+'speed_trap.html'
        print 'trying',url
        content=urllib.urlopen(url).read()
        page=lxml.html.fromstring(content)
        for table in page.findall('.//table'):
            for row in table.findall('.//tr')[1:]:
                #print flatten(row)
                cells=row.findall('.//td')
                
                data = {'year':year, 'race':quali[1],'session':quali[2], 'pos':flatten(cells[0]), 'driverNum':flatten(cells[1]), 'driverName':flatten(cells[2]), 'timeOfDay':flatten(cells[3]), 'qspeed':flatten(cells[4])}
                bigdata.append(data.copy())
                if len(bigdata)>1000:
                    #scraperwiki.sqlite.save(unique_keys=['year','race','driverNum'], table_name=tn, data=bigdata)
                    scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','session'], table_name=tn, data=bigdata)
                    bigdata=[]
    #scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','session'], table_name=tn, data=bigdata)
    scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','session'], table_name=tn, data=bigdata)


def qSectorsScraper(sessions,tn,year):
    """ Scrape best_sector_times.html results """
    dropper(tn)
    bigdata=[]
    for quali in sessions:
        url=quali[0]+'best_sector_times.html'
        print 'trying',url
        content=urllib.urlopen(url).read()
        page=lxml.html.fromstring(content)
        sector=0
        for table in page.findall('.//table'):
            sector=sector+1
            for row in table.findall('.//tr')[2:]:
                #print row,flatten(row)
                cells=row.findall('.//td')
                
                data={'year':year, 'race':quali[1],'session':quali[2], 'pos':flatten(cells[0]), 'driverNum':flatten(cells[1]), 'driverName':flatten(cells[2]), 'sector':sector, 'sectortime':flatten(cells[3])}
                bigdata.append(data.copy())
                if len(bigdata)>1000:
                    scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','sector','session'], table_name=tn, data=bigdata)
                    #scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','sector'], table_name=tn, data=bigdata)
                    bigdata=[]
    #scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','sector'], table_name=tn, data=bigdata)
    scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','sector','session'], table_name=tn, data=bigdata)

    
def qResults(qualis,year):
    """ Scrape qualifying session results.html results """
    tn='qualiResults'
    dropper(tn)
    bigdata=[]
    for quali in qualis:
        url=quali[0]+'results.html'
        print 'trying',url,
        content=urllib.urlopen(url).read()
        page=lxml.html.fromstring(content)
        for table in page.findall('.//table'):
            for row in table.findall('.//tr')[1:]:
                #print flatten(row)
                cells=row.findall('.//td')
                if flatten(cells[0])=='':continue

                if year>2005:
                	data={'year':year,'race':quali[1],'session':quali[2], 'pos':flatten(cells[0]), 'driverNum':flatten(cells[1]), 'driverName':flatten(cells[2]), 'team':flatten(cells[3]), 'q1natTime':flatten(cells[4]), 'q1time':getTime(flatten(cells[4])), 'q2natTime':flatten(cells[5]), 'q2time':getTime(flatten(cells[5])), 'q3natTime':flatten(cells[6]), 'q3time':getTime(flatten(cells[6])), 'qlaps':flatten(cells[7])}
                	bigdata.append(data.copy())
                	if len(bigdata)>1000:
                		scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','session'], table_name=tn, data=bigdata)
                		bigdata=[]
                else:
                	tn='QualiResultsto2005'
                	data={'year':year,'session':quali[2],'race':quali[2], 'pos':flatten(cells[0]), 'driverNum':flatten(cells[1]), 'driverName':flatten(cells[2]), 'team':flatten(cells[3]), 'natTime':flatten(cells[4]), 'time':getTime(flatten(cells[4]))}
                	bigdata.append(data.copy())
                	if len(bigdata)>1000:
                		scraperwiki.sqlite.save(unique_keys=['year','race','driverNum'], table_name=tn, data=bigdata)
                		bigdata=[]
                     	
    scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','session'], table_name=tn, data=bigdata)

def practiceResults(sessions,tn,year):
    """ Scrape practice session results.html results """
    dropper(tn)
    bigdata=[]
    for session in sessions:
        url=session[0]+'results.html'
        print 'trying',url
        content=urllib.urlopen(url).read()
        page=lxml.html.fromstring(content)
        for table in page.findall('.//table'):
            for row in table.findall('.//tr')[1:]:
                #print flatten(row)
                cells=row.findall('.//td')
                
                if flatten(cells[0])=="1":
                    gap=0.0
                    natGap=0.0
                else:
                    natGap=flatten(cells[5])
                    gap=getTime(flatten(cells[5]))
                #print natGap,gap
                data={'year':tryint(year),'race':session[1],'session':session[2], 'pos':tryint(flatten(cells[0])), 'driverNum':flatten(cells[1]), 'driverName':flatten(cells[2]), 'team':flatten(cells[3]), 'natTime':flatten(cells[4]), 'time':getTime(flatten(cells[4])), 'natGap':natGap if natGap!='' else float('nan'), 'gap':gap, 'laps':tryint(flatten(cells[6]))}
                #scraperwiki.sqlite.save(unique_keys=[], table_name=tn, data=data)
                bigdata.append(data.copy())
                if len(bigdata)>1000:
                    #scraperwiki.sqlite.save(unique_keys=['year','race','driverNum'], table_name=tn, data=bigdata)
                    scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','session'], table_name=tn, data=bigdata)
                    bigdata=[]
    #scraperwiki.sqlite.save(unique_keys=['year','race','driverNum'], table_name=tn, data=bigdata)
    scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','session'], table_name=tn, data=bigdata)

	
def resScraper(races,year):
    """ Scrape race session results.html results """
    tn='raceResults'
    dropper(tn)
    bigdata=[]
    raceNum=0
    for race in races:
        raceNum=raceNum+1
        url=race[0]+'results.html'
        print 'trying',url
        content=urllib.urlopen(url).read()
        page=lxml.html.fromstring(content)
        for table in page.findall('.//table'):
            for row in table.findall('.//tr')[1:]:
                #print flatten(row)
                cells=row.findall('.//td')
                
                try:
                	data={'year':year,'raceNum':raceNum,'race':race[1],'session':race[2], 'pos':flatten(cells[0]), 'driverNum':flatten(cells[1]), 'driverName':flatten(cells[2]), 'team':flatten(cells[3]), 'laps':flatten(cells[4]), 'timeOrRetired':flatten(cells[5]), 'grid':flatten(cells[6]), 'points':flatten(cells[7])}
                	#scraperwiki.sqlite.save(unique_keys=[], table_name=tn, data=data)
                	bigdata.append(data.copy())
                	if len(bigdata)>1000:
                		#scraperwiki.sqlite.save(unique_keys=['year','race','driverNum'], table_name=tn, data=bigdata)
                		scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','session'], table_name=tn, data=bigdata)
                		bigdata=[]
                except: pass
    #scraperwiki.sqlite.save(unique_keys=['year','race','driverNum'], table_name=tn, data=bigdata)
    scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','session'], table_name=tn, data=bigdata)
    #fout.close()

def pitScraper(races,year):
    """ Scrape pit_stop_summary.html results """
    tn='racePits'
    dropper(tn)
    bigdata=[]
    raceNum=0
    for race in races:
        raceNum=raceNum+1
        url=race[0]+'pit_stop_summary.html'
        print 'trying',url
        content=urllib.urlopen(url).read()
        page=lxml.html.fromstring(content)
        for table in page.findall('.//table'):
            for row in table.findall('.//tr')[1:]:
                #print flatten(row)
                cells=row.findall('.//td')
                
                try:
                	data={'year':year,'raceNum':raceNum,'race':race[1], 'stops':flatten(cells[0]), 'driverNum':flatten(cells[1]), 'driverName':flatten(cells[2]), 'team':flatten(cells[3]), 'lap':flatten(cells[4]), 'timeOfDay':flatten(cells[5]), 'natPitTime':flatten(cells[6]), 'pitTime':getTime(flatten(cells[6])), 'natTotalPitTime':flatten(cells[7]), 'totalPitTime':getTime(flatten(cells[7]))}
                	#scraperwiki.sqlite.save(unique_keys=[], table_name=tn, data=data)
                	bigdata.append(data.copy())
                	if len(bigdata)>1000:
                		scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','stops'], table_name=tn, data=bigdata)
                		bigdata=[]
                except:pass
    scraperwiki.sqlite.save(unique_keys=['year','race','driverNum','stops'], table_name=tn, data=bigdata)

def flapScraper(races,year):
    """ Scrape fastest_laps.html results """
    tn='raceFastlaps'
    dropper(tn)
    bigdata=[]
    raceNum=0
    for race in races:
        raceNum=raceNum+1
        url=race[0]+'fastest_laps.html'
        print 'trying',url
        content=urllib.urlopen(url).read()
        page=lxml.html.fromstring(content)
        for table in page.findall('.//table'):
            for row in table.findall('.//tr')[1:]:
                #print flatten(row)
                cells=row.findall('.//td')
                try:
                	data={'year':year,'raceNum':raceNum,'race':race[1], 'pos':flatten(cells[0]), 'driverNum':flatten(cells[1]), 'driverName':flatten(cells[2]), 'team':flatten(cells[3]), 'lap':flatten(cells[4]), 'timeOfDay':flatten(cells[5]), 'speed':flatten(cells[6]), 'natTime':flatten(cells[7]), 'stime':getTime(flatten(cells[7]))}
                	#scraperwiki.sqlite.save(unique_keys=[], table_name=tn, data=data)
                	bigdata.append(data.copy())
                	if len(bigdata)>1000:
                		scraperwiki.sqlite.save(unique_keys=['year','race','driverNum'], table_name=tn, data=bigdata)
                		bigdata=[]
                except: pass
    scraperwiki.sqlite.save(unique_keys=['year','race','driverNum'], table_name=tn, data=bigdata)





def liaparse(ul):
    d=[]
    lis=ul.findall('.//li')
    for li in lis:
        a=li.find('a')
        u=a.get('href')
        r=flatten(li)
        print 'liap:',u,r
        d.append((u,r))
    return d


def yearGrabber(year):
    urlstub='http://www.formula1.com/results/season'
    url='/'.join( [urlstub,str(year) ])
    print 'trying',url
    content=urllib.urlopen(url).read()
    page=lxml.html.fromstring(content)
    uls =page.findall('.//ul')

    ah=liaparse(uls[14])

    s1=[]
    s2=[]
    s3=[]
    p=[]
    qualis=[]
    races=[]

    for (u,r) in ah[1:]:
        print 'a:',u,r,':a'
        print 'c: trying',u,':c'
        content2=urllib.urlopen('http://formula1.com'+u).read()

        page2=lxml.html.fromstring(content2)
        uls2 =page2.findall('.//ul')
        #print len(uls2),content

        ah2=liaparse(uls2[15])
        for (u2,r2) in ah2:
            print '\tb:',u2,r2,':b'
            if 'LIVE' in r2: break
            if '1' in r2:
                s1.append(['http://formula1.com'+u2,r.strip(),r2])
            elif '2' in r2:
                s2.append(['http://formula1.com'+u2,r.strip(),r2])
            elif '3' in r2 or 'PRACTICE' in r2:
                s3.append(['http://formula1.com'+u2,r.strip(),r2])
            elif year<=2005 and 'PRACTICE' in r2:
            	p.append(['http://formula1.com'+u2,r.strip(),r2])
            elif 'QUALI' in r2:
                qualis.append(['http://formula1.com'+u2,r.strip(),r2])
            elif 'RACE' in r2:
                races.append(['http://formula1.com'+u2,r.strip(),r2])
        #print s1,s2,s3,qualis,races

    if scraping==1:scrapeset=["P1","P2","P3"]
    if scraping==2:scrapeset=["P1","P2"]
    if scraping==3:scrapeset=["P3"]
    if scraping==4:scrapeset=["P3","Q"]
    if scraping==5:scrapeset=["Q"]
    if scraping==6:scrapeset=["R"]
    if scraping==7:scrapeset=["P1","P2","P3","Q"]
    if scraping==8:scrapeset=["P1","P2","P3","Q","R"]
    if scraping==9:scrapeset=["P","P1","P2","P3","Q","R"]


    #Practice: best_sector_times.html, speed_trap.html
    #s1=[]
    if (latest>0): s1=[s1[-latest]]
    #s2=[]
    if (latest>0): s2=[s2[-latest]]
    #s3=[]
    if (latest>0): s3=[s3[-latest]]

    #best_sector_times.html, speed_trap.html, results.html
    #qualis=[]
    if (latest>0): qualis=[qualis[-latest]]

    #pit_stop_summary.html, fastest_laps.html, results.html
    #races=[]
    if (latest>0): races=[races[-latest]]

    print ("Race")
    if "R" in scrapeset:
        if year>=2004: flapScraper(races,year)
        resScraper(races,year)
        if year>=2004: pitScraper(races,year)

    print("Quali")
    if "Q" in scrapeset:
        qSpeedScraper(qualis,'Speeds',year)
        qResults(qualis,year)
        qSectorsScraper(qualis,'Sectors',year)


    print("P1")
    if "P1" in scrapeset:
    	print "trying for ",s1
        qSpeedScraper(s1,"Speeds",year)
        qSectorsScraper(s1,"Sectors",year)
        practiceResults(s1,"pResults",year)

    print("P2")
    if "P2" in scrapeset:
    	print "trying for ",s2
        qSpeedScraper(s2,"Speeds",year)
        qSectorsScraper(s2,"Sectors",year)
        practiceResults(s2,"pResults",year)

    print("P3")
    if "P3" in scrapeset:
        qSpeedScraper(s3,"Speeds",year)
        qSectorsScraper(s3,"Sectors",year)
        practiceResults(s3,"pResults",year)

    if "P" in scrapeset:
        qSpeedScraper(p,"Speeds",year)
        qSectorsScraper(p,"Sectors",year)
        practiceResults(p,"pResults",year)
    	
for y in years:
    print("trying {}".format(y))
    yearGrabber(y)
    #We don't want to keep dropping tables at each pass...
    nodrop=1

print("all done...")
