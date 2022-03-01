package com.pluralsight.repository;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.pluralsight.repository.util.RideRowMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import com.pluralsight.model.Ride;

@Repository("rideRepository")
public class RideRepositoryImpl implements RideRepository {
	final String INSERT_QUERY = "INSERT INTO ride (name, duration) VALUES (?, ?)";

	final String SELECT_BY_ID = "SELECT * FROM ride WHERE id = ?";

	final String UPDATE_QUERY = "UPDATE ride SET name = ?, duration = ? WHERE id = ?";

	final String BATCH_UPDATE_QUERY = "UPDATE ride SET ride_date = ? WHERE id = ?";

	final String DELETE_QUERY = "DELETE FROM ride WHERE id = ?";

	final String DELETE_QUERY_NAMED = "DELETE FROM ride WHERE id = :id";

	 @Autowired
	 private JdbcTemplate jdbcTemplate;

	@Override
	public List<Ride> getRides() {
		List<Ride> rides = jdbcTemplate.query("SELECT * FROM ride", new RideRowMapper());

		return rides;
	}

	@Override
	public Ride createRide(Ride ride) {
		// jdbcTemplate.update(INSERT_QUERY, ride.getName(), ride.getDuration());

		KeyHolder keyHolder = new GeneratedKeyHolder();
		jdbcTemplate.update(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				PreparedStatement ps = con.prepareStatement(INSERT_QUERY, new String[]{"id"});
				ps.setString(1, ride.getName());
				ps.setInt(2, ride.getDuration());
				return ps;
			}
		}, keyHolder);

		Number id = keyHolder.getKey();

		/**
		System.out.println("Create Ride, beginning of the method");
		SimpleJdbcInsert insert = new SimpleJdbcInsert(jdbcTemplate);

		List<String> columns = new ArrayList<>();
		columns.add("name");
		columns.add("duration");

		insert.setTableName("ride");
		insert.setColumnNames(columns);

		Map<String, Object> data = new HashMap<>();
		data.put("name", ride.getName());
		data.put("duration", Integer.valueOf(ride.getDuration()));

		insert.setGeneratedKeyName("id");

		System.out.println("Before execution data: " + data);
		System.out.println("before execution columns: " + columns);
		Number key = insert.executeAndReturnKey(data);
		System.out.println("Key for inserted value is: " + key);
		*/
		return getRide(id.intValue());

	}

	@Override
	public Ride getRide(int id) {
		Ride ride = jdbcTemplate.queryForObject(SELECT_BY_ID, new RideRowMapper(), id);
		return ride;
	}

	@Override
	public Ride updateRide(Ride ride) {
		jdbcTemplate.update(UPDATE_QUERY, ride.getName(), ride.getDuration(), ride.getId());
		return ride;
	}

	@Override
	public void updateRides(List<Object[]> pairs) {
		jdbcTemplate.batchUpdate(BATCH_UPDATE_QUERY, pairs);
	}

	@Override
	public void deleteRide(Integer id) {
		jdbcTemplate.update(DELETE_QUERY, id);
		/**
		System.out.println("Inside Delete Ride method");

		NamedParameterJdbcTemplate namedJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
		System.out.println("After instantion of namedJdbcTemplate");
		Map<String, Object> paramMap = new HashMap<>();
		paramMap.put("id", id);

		System.out.println("Named template: " + namedJdbcTemplate);
		namedJdbcTemplate.update(DELETE_QUERY_NAMED, paramMap);
		System.out.println("after update");
		*/
	}
}
