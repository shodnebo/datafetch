from datetime import datetime
from datetime import date
from ftplib import FTP

def get_ftp_files(host, path, from_date):
    ftp = FTP(host)
    ftp.login()  # user anonymous, passwd anonymous@
    ftp.cwd(path)
    data = []
    ftp.dir(data.append)
    filelist = {}
    for line in data:
        col = line.split()
        datestr = ' '.join(line.split()[5:8])
        file_path = 'ftp://'+host+path+'/'+col[8]
        try:
            file_date = datetime.strptime(datestr, '%b %d %H:%M')
            file_date = file_date.replace(year=date.today().year)
        except (ValueError) as e:
            file_date = datetime.strptime(datestr, '%b %d %Y')
        #if "2017tx" not in file_path:
        #    continue;
        if from_date <= file_date:
            filelist[file_path] = file_date
    ftp.quit()
    return filelist