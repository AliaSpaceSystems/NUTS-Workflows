docker exec -i $(docker-compose ps -q postgres) psql -U XXXXXXXXXX nuts_db < sql/nuts_ddl.sql
