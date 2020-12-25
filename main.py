from flask import Flask, render_template, request, current_app, redirect, url_for, flash, get_flashed_messages
from flask_sqlalchemy import SQLAlchemy
from elasticsearch import Elasticsearch

app = Flask(__name__)
app.config.from_object(__name__)
app.config['SECRET_KEY'] = 'You shall not pass'
app.elasticsearch = Elasticsearch('http://localhost:9200')
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///base.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
db.create_all()


def add_to_index(index, model):
    """
    Добавить к индексу elasticsearch запись из бд
    :param index: индекс elasticsearch
    :param model: модель базы данных
    """
    if not current_app.elasticsearch:
        return
    payload = {}
    print(model.__searchable__)
    for field in model.__searchable__:
        print(field)
        payload[field] = getattr(model, field)
    current_app.elasticsearch.index(index=index, doc_type=index, id=model.id, body=payload)


def remove_from_index(index, model):
    """
    Удалить из индекса elasticsearch запись из бд
    :param index: индекс elasticsearch
    :param model: модель базы данных
    """
    if not current_app.elasticsearch:
        return
    current_app.elasticsearch.delete(index=index, doc_type=index, id=model.id)


def query_index(index, query, page, per_page):
    """
    Поиск данных в индексе по запросу
    :param index: индекс elasticsearch
    :param query: пользовательский запрос
    :param page: номер страницы
    :param per_page: количество результатов на одну страницу
    :return:
    """
    if not current_app.elasticsearch:
        return [], 0
    search = current_app.elasticsearch.search(
        index=index,
        body={'query':
                  {'multi_match':
                       {'query': query,
                        'fields': ['*']
                        }
                   },
              'from': (page - 1) * per_page,
              'size': per_page})
    ids = [int(hit['_id']) for hit in search['hits']['hits']]
    return ids, search['hits']['total']['value']


class SearchableMixin(object):
    """
    Вспомогательный клаcс для связи модели бд и elasticsearch
    """

    @classmethod
    def search(cls, expression, page, per_page):
        ids, total = query_index(cls.__tablename__, expression, page, per_page)
        if total == 0:
            return cls.query.filter_by(id=0), 0
        when = []
        for i in range(len(ids)):
            when.append((ids[i], i))
        return cls.query.filter(cls.id.in_(ids)).order_by(cls.created_date.desc(), db.case(when, value=cls.id)), total

    @classmethod
    def before_commit(cls, session):
        session._changes = {
            'add': list(session.new),
            'update': list(session.dirty),
            'delete': list(session.deleted)
        }

    @classmethod
    def after_commit(cls, session):
        for obj in session._changes['add']:
            if isinstance(obj, SearchableMixin):
                add_to_index(obj.__tablename__, obj)
        for obj in session._changes['update']:
            if isinstance(obj, SearchableMixin):
                add_to_index(obj.__tablename__, obj)
        for obj in session._changes['delete']:
            if isinstance(obj, SearchableMixin):
                remove_from_index(obj.__tablename__, obj)
        session._changes = None

    @classmethod
    def reindex(cls):
        for obj in cls.query:
            add_to_index(cls.__tablename__, obj)


db.event.listen(db.session, 'before_commit', SearchableMixin.before_commit)
db.event.listen(db.session, 'after_commit', SearchableMixin.after_commit)


class Document(SearchableMixin, db.Model):
    """
    Модель документа
    """
    __searchable__ = ['text']
    id = db.Column(db.Integer, primary_key=True)
    rubrics = db.Column(db.String)
    text = db.Column(db.Text, unique=True)
    created_date = db.Column(db.DateTime)


@app.route('/', methods=["GET", "POST"])
def search_page():
    """
    Главная страница с формой для ввода запроса
    """
    if request.method == 'GET':
        if request.args.get('q'):
            query, total = Document.search(request.args.get('q'), 1, 20)
            return render_template('results.html', docs=query.all())
    return render_template('search_page.html')


@app.route('/results', methods=['GET', 'POST'])
def results_page():
    """
    Страница с выдачей результатов запроса
    """
    query, total = Document.search(request.args.get('q'), 1, 20)
    return render_template('result.html', docs=query.all())


@app.route('/document/<id>', methods=['GET', 'POST'])
def document_page(id):
    """
    Страница с содержанием документа
    :param id: id документа в бд
    """
    query = Document.query.filter_by(id=id).first()
    if request.method == 'POST':
        if request.form['delete']:
            flash('Удалено')
            remove_from_index('document', query)
    return render_template('docpage.html', id=id, doc=query)


if __name__ == '__main__':
    app.run(debug=True)
