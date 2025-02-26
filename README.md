# Проект спринта "Аналитические базы данных"

### Описание
Репозиторий предназначен для сдачи проекта српинта "Аналитические базы данных"

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-start-sprint-analytical-databases` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой локальный компьютер, в качестве пароля укажите ваш `Access Token` (получить нужно на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-start-sprint-analytical-databases.git`
3. Перейдите в директорию с проектом: 
	* `cd de-start-sprint-analytical-databases`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GutHub-аккаунте:
	* `git push origin main`

### Структура репозитория
- `/src/dags`

### Как запустить контейнер
Запустите локально команду:
```
docker run \
-d \
-p 3000:3000 \
-p 3002:3002 \
-p 15432:5432 \
--mount src=airflow_sp5,target=/opt/airflow \
--mount src=lesson_sp5,target=/lessons \
--mount src=db_sp5,target=/var/lib/postgresql/data \
--name=de-project-adb-server-local \
cr.yandex/crp1r8pht0n0gl25aug1/de-pg-cr-af:latest
```

После того как запустится контейнер, вам будут доступны:
- Airflow
	- `localhost:3000/airflow`
- БД
	- `jovyan:jovyan@localhost:15432/de`
