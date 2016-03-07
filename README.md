# flask-webcrawler
simple webcrawler script in python with web interface in Flask

для работы приложения нужны две библиотеки - flask и requests

запустить скрипт можно двумя способами

1. python ww2.py URL > output.cvs

программа запишет данные в stdout и завершится

2. если запустить скрипт без параметра, то запустится http сервер
в браузере откройте http://217.0.0.1:5000 , введите урл и ждите окончания парсинга

python ww2.py --help   - покажет справочную инфу

для парсинга укажите вот этот урл - http://www.yell.ru/spb/top/restorany/
