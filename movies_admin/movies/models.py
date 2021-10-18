from uuid import uuid4

from django.db import models
from django.core.validators import MinValueValidator
from django.utils.translation import gettext_lazy as _


class TimeStampedIdMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid4)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Filmwork(TimeStampedIdMixin, models.Model):
    title = models.CharField(_("Название"), max_length=255)
    description = models.TextField(_("Описание"), blank=True, null=True)
    creation_date = models.DateField(_("Дата создания"), blank=True, null=True)
    certificate = models.TextField(_("Сертификат"), blank=True, null=True)
    file_path = models.CharField(_("Файл"), max_length=100, blank=True, null=True)
    rating = models.FloatField(
        _("Рейтинг"), validators=[MinValueValidator(0)], blank=True, null=True
    )
    type = models.CharField(_("Тип"), max_length=20)
    genres = models.ManyToManyField(to="movies.Genre", through="movies.GenreFilmwork")
    persons = models.ManyToManyField(
        to="movies.Person", through="movies.PersonFilmwork"
    )

    def __str__(self):
        return self.title

    class Meta:
        verbose_name = _("Кинопроизведение")
        verbose_name_plural = _("Кинопроизведения")
        db_table = 'content"."filmwork'


class Genre(TimeStampedIdMixin, models.Model):
    name = models.CharField(_("Название"), max_length=255)
    description = models.TextField(_("Описание"), blank=True, null=True)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = _("Жанр")
        verbose_name_plural = _("Жанры")
        db_table = 'content"."genre'


class Person(TimeStampedIdMixin, models.Model):
    full_name = models.CharField(_("Полное имя"), max_length=255)
    birth_date = models.DateField(_("Дата рождения"), blank=True, null=True)

    def __str__(self):
        return self.full_name

    class Meta:
        verbose_name = _("Актер")
        verbose_name_plural = _("Актеры")
        db_table = 'content"."person'


class GenreFilmwork(models.Model):
    filmwork = models.ForeignKey(to="movies.Filmwork", on_delete=models.CASCADE)
    genre = models.ForeignKey(to="movies.Genre", on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = _("Жанр")
        verbose_name_plural = _("Жанры")
        db_table = 'content"."genre_filmwork'
        unique_together = (("filmwork", "genre"),)


class PersonFilmwork(models.Model):
    filmwork = models.ForeignKey(to="movies.Filmwork", on_delete=models.CASCADE)
    person = models.ForeignKey(to="movies.Person", on_delete=models.CASCADE)
    role = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = _("Актер")
        verbose_name_plural = _("Актеры")
        db_table = 'content"."person_filmwork'
        unique_together = (("filmwork", "person", "role"),)
