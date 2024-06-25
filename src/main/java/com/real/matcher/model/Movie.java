package com.real.matcher.model;

import java.util.ArrayList;
import java.util.List;

public class Movie {
    private int id;
    private String title;
    private String year;
    private List<String> actors = new ArrayList<>();
    private List<String> directors = new ArrayList<>();

    public int getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public List<String> getDirectors() {
        return directors;
    }

    public String getYear() {
        return year;
    }

    public List<String> getActors() {
        return actors;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public void setActors(List<String> actors) {
        this.actors = actors;
    }

    public void setDirectors(List<String> directors) {
        this.directors = directors;
    }

    public void appendToActors(String actor) {
        this.actors.add(actor);
    }

    public void appendToDirectors(String director) {
        this.directors.add(director);
    }

    public Movie merge(Movie movie) {

        if(!movie.getActors().isEmpty()) {
            if(this.actors.isEmpty()) {
                this.actors = movie.getActors();
            } else {
                this.actors.addAll(movie.getActors());
            }
        }

        if(!movie.getDirectors().isEmpty()) {
            if(this.directors.isEmpty()) {
                this.directors = movie.getDirectors();
            } else {
                this.directors.addAll(movie.getDirectors());
            }
        }

        if(null != movie.getYear()) {
            this.year = movie.getYear();
        }

        return movie;
    }
}