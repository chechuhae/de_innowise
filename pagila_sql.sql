SELECT category, count(fid) AS quantity FROM film_list
GROUP BY category
ORDER BY quantity DESC;

SELECT first_name, last_name, sum(rental_duration) AS q_days FROM 
actor INNER JOIN film_actor on actor.actor_id = film_actor.actor_id
INNER JOIN film on film.film_id = film_actor.film_id
GROUP BY first_name, last_name
ORDER BY q_days DESC
limit 10;

SELECT category, sum(film.rental_duration * film.rental_rate) as total_sum FROM film_list
INNER JOIN film on fid=film_id
GROUP BY category
ORDER BY total_sum DESC
limit 1;

SELECT title FROM film
LEFT JOIN inventory on film.film_id = inventory.film_id
WHERE inventory_id IS NULL;

SELECT * FROM 
	(SELECT first_name, last_name, rank() OVER (ORDER BY count(fid) DESC) AS rank
	FROM (film_list
	INNER JOIN film_actor on fid = film_id
	INNER JOIN actor on film_actor.actor_id = actor.actor_id)
	WHERE category = 'Children' 
	GROUP BY first_name, last_name) new_table
WHERE rank <= 3;

SELECT city, sum(active) AS active, count(active) - sum(active) AS inactive 
FROM customer 
INNER JOIN customer_list ON customer.customer_id = customer_list.id
GROUP BY city
ORDER BY inactive DESC;

WITH t_1 AS
	(SELECT city, category, SUM(film.rental_duration) AS q_hours  
	FROM film_list
	INNER JOIN film ON film.film_id = film_list.fid
	INNER JOIN inventory ON inventory.film_id = film.film_id
	INNER JOIN rental ON rental.inventory_id = inventory.inventory_id
	INNER JOIN customer ON customer.customer_id = rental.customer_id
	INNER JOIN address ON address.address_id = customer.address_id
	INNER JOIN city ON city.city_id = address.city_id
	GROUP BY city, category
	ORDER BY q_hours DESC)

(SELECT t_1.category FROM t_1
WHERE city LIKE 'A%%'
LIMIT 1)

UNION

(SELECT category FROM t_1
WHERE city LIKE '%-%'
LIMIT 1)