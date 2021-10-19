from django.contrib import admin
from movies.models import Filmwork, Genre, GenreFilmwork, Person, PersonFilmwork


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    fields = ("genre",)
    extra = 0


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork
    fields = ("person", "role")
    extra = 0


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    list_display = ("title", "type", "creation_date", "rating")
    readonly_fields = ("id",)
    ordering = ("title",)

    inlines = [GenreFilmworkInline, PersonFilmworkInline]


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ("name",)
    fields = ("name", "description")
    ordering = ("name",)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ("full_name",)
    fields = ("full_name", "birth_date")
    ordering = ("full_name",)
