BEGIN TRANSACTION;
ALTER TABLE film ADD COLUMN category_id VARCHAR(30);
COMMIT TRANSACTION;

UPDATE film SET category_id =
(
  SELECT film_category.category_id
  FROM film_category
  WHERE film_category.film_id = film.film_id
);

DROP TABLE film_category