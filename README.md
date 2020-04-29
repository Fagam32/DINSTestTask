# DINSTestTask

## Требования
 + Java 8
 + Docker
 + Docker-compose
 + Maven
 
 ## Запуск
 Необходимые порты для запуска на localhost: `3306` `9092` `2181`. Для запуска тестов необходим еще `4445`
 
 Также джаве необходимы права для сниффинга
 
 `sudo setcap cap_net_raw,cap_net_admin=eip /path/to/java`
 
 Клонируем репозиторий
 ```
 mkdir ~/Task
 cd ~/Task
 git clone https://github.com/Fagam32/TrafficSniffer
 cd ~/Task/TrafficSniffer
 ```
 Создаем jar
 
 `mvn clean package`
 
 
 Собираем и запускаем контейнеры
 
 ```
 docker-compose build
 docker-compose up
 ```
 
 Запускаем проект:
 
 `java -jar target/name_of_jar [options]`
 
 
 Опции:
 + `from=ip:socket` ИЛИ `from=ip`
 + `to=ip:socket` ИЛИ `to=ip`
 + Можно использовать обе сразу
 + Если опций нет, то считается весь трафик

### Общее
 + Все тесты и разработка делались в Linux Mint 19.3
 + В целом, считаются все пакеты, включая "рукопожатия" по TCP
 + Логи идут в папку /projectPath/logs/
 + Нет гарантии на то, что сообщение дойдет. Fire and forget

 
