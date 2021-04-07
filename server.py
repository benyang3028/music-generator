"""
Columbia's COMS W4111.001 Introduction to Databases
Example Webserver
To run locally:
    python server.py
Go to http://localhost:8111 in your browser.
A debugger such as "pdb" may be helpful for debugging.
Read about it online.
"""
import os
  # accessible as a variable in index.html:
import random
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)

app.config["TEMPLATES_AUTO_RELOAD"] = True
#
# The following is a dummy URI that does not connect to a valid database. You will need to modify it to connect to your Part 2 database in order to use the data.
#
# XXX: The URI should be in the format of: 
#
#     postgresql://USER:PASSWORD@34.73.36.248/project1
#
# For example, if you had username zy2431 and password 123123, then the following line would be:
#
#     DATABASEURI = "postgresql://zy2431:123123@34.73.36.248/project1"
#
DATABASEURI = "postgresql://tl2977:wien501@34.73.36.248/project1" # Modify this with your own credentials you received from Joseph!


#
# This line creates a database engine that knows how to connect to the URI above.
#
engine = create_engine(DATABASEURI)

#
# Example of running queries in your database
# Note that this will probably not work if you already have a table named 'test' in your database, containing meaningful data. This is only an example showing you how to run queries in your database using SQLAlchemy.
#

engine.execute("""CREATE TABLE IF NOT EXISTS test (
  id serial,
  name text
);""")
#engine.execute("""INSERT INTO test(name, ) VALUES ('grace hopper'), ('alan turing'), ('ada lovelace');""")


@app.before_request
def before_request():
  """
  This function is run at the beginning of every web request 
  (every time you enter an address in the web browser).
  We use it to setup a database connection that can be used throughout the request.

  The variable g is globally accessible.
  """
  try:
    g.conn = engine.connect()
  except:
    print("uh oh, problem connecting to database")
    import traceback; traceback.print_exc()
    g.conn = None

@app.teardown_request
def teardown_request(exception):
  """
  At the end of the web request, this makes sure to close the database connection.
  If you don't, the database could run out of memory!
  """
  try:
    g.conn.close()
  except Exception as e:
    pass


#
# @app.route is a decorator around index() that means:
#   run index() whenever the user tries to access the "/" path using a GET request
#
# If you wanted the user to go to, for example, localhost:8111/foobar/ with POST or GET then you could use:
#
#       @app.route("/foobar/", methods=["POST", "GET"])
#
# PROTIP: (the trailing / in the path is important)
# 
# see for routing: https://flask.palletsprojects.com/en/1.1.x/quickstart/#routing
# see for decorators: http://simeonfranklin.com/blog/2012/jul/1/python-decorators-in-12-steps/
#
@app.route('/')
def index():
  """
  request is a special object that Flask provides to access web request information:

  request.method:   "GET" or "POST"
  request.form:     if the browser submitted a form, this contains the data in the form
  request.args:     dictionary of URL arguments, e.g., {a:1, b:2} for http://localhost?a=1&b=2

  See its API: https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data
  """

  # DEBUG: this is debugging code to see what request looks like
  print(request.args)


  #
  # example of a database query
  #
  cursor = g.conn.execute("SELECT name FROM test")
  print(cursor)
  names = []
  for result in cursor:
    names.append(result['name'])  # can also be accessed using result[0]
  cursor.close()


  #
  # Flask uses Jinja templates, which is an extension to HTML where you can
  # pass data to a template and dynamically generate HTML based on the data
  # (you can think of it as simple PHP)
  # documentation: https://realpython.com/primer-on-jinja-templating/
  #
  # You can see an example template in templates/index.html
  #
  # context are the variables that are passed to the template.
  # for example, "data" key in the context variable defined below will be 
  # accessible as a variable in index.html:
  #
  #     # will print: [u'grace hopper', u'alan turing', u'ada lovelace']
  #     <div>{{data}}</div>
  #     
  #     # creates a <div> tag for each element in data
  #     # will print: 
  #     #
  #     #   <div>grace hopper</div>
  #     #   <div>alan turing</div>
  #     #   <div>ada lovelace</div>
  #     #
  #     {% for n in data %}
  #     <div>{{n}}</div>
  #     {% endfor %}
  #
  #
  context = dict(data = names)

  #
  # render_template looks in the templates/ folder for files.
  # for example, the below file reads template/index.html
  #
  return render_template("index.html", **context)

#
# This is an example of a different path.  You can see it at:
# 
#     localhost:8111/another
#
# Notice that the function name is another() rather than index()
# The functions for each app.route need to have different names
#
@app.route('/another')
def another():
  cursor = g.conn.execute('SELECT name as artist_name, concert.* FROM ((SELECT artists.*, concertID FROM artists JOIN performs USING(artistid)) as a JOIN concert USING(concertid)) ORDER BY date DESC;')

  _fields = cursor.keys()

  fields = []
  for x in _fields:
    if x not in fields and x != "concertid":
      fields.append(x)
  
  output = []
  for result in cursor:
    row = ()
    for f in fields:
      row += (result[f],)

    output.append(row)

  fields = [(f,) for f in fields]
  cursor.close()
  context = dict(table=output, fields=fields)
  return render_template("another.html", **context)

@app.route('/concert', methods=['GET', 'POST'])
def concert():
  search = request.form['search']

  query = 'SELECT name as artist_name, concert.* FROM ((SELECT artists.*, concertID FROM artists JOIN performs USING(artistid)) as a JOIN concert USING(concertid)) WHERE name = (%s) ORDER BY date DESC;'
  cursor = g.conn.execute(query, (search,))

  _fields = cursor.keys()

  fields = []
  for x in _fields:
    if x not in fields and x != "concertid":
      fields.append(x)
  
  output = []
  for result in cursor:
    row = ()
    for f in fields:
      row += (result[f],)

    output.append(row)

  fields = [(f,) for f in fields]
  cursor.close()
  context = dict(search=str(search), table=output, fields=fields)
  return render_template("concert.html", **context)

@app.route('/showtable', methods=['GET', 'POST'])
def showtable():
  category = request.form['showtable']

  if category == 'songs':
    cursor = g.conn.execute('select title, array_agg(name) as artists, duration, language from (select * from artists join writes using(artistid)) as a join songs using(songid) group by title, duration, language order by title asc')
  elif category == 'albums':
    cursor = g.conn.execute('select title, artist[1], genre, releasedate from (select distinct title, array_agg(name) as artist, genre, releasedate from (select albumid, name from (select * from (select * from artists join writes using(artistid)) as a join songs using(songid)) as b join contains using(songid)) as c join album using(albumid) group by title, genre, releasedate) as d')
  elif category == 'artists':
    cursor = g.conn.execute('select name, count(*) as followers FROM artists JOIN follows USING(artistID) GROUP BY artistid, name ORDER BY followers DESC')

  _fields = cursor.keys()

  fields = []
  for x in _fields:
    if x not in fields:
      fields.append(x)
  
  output = []
  for result in cursor:
    row = ()
    for f in fields:
      row += (result[f],)

    output.append(row)

  fields = [(f,) for f in fields]

  cursor.close()
  context = dict(search=str(category), table=output, fields=fields)
  return render_template("showtable.html", **context)

@app.route('/ratinghome', methods=['GET', 'POST'])
def ratinghome():
  cursor = g.conn.execute('select songid, title, array_agg(name) as artists, duration, language from (select * from artists join writes using(artistid)) as a join songs using(songid) group by songid, title, duration, language order by title asc')
  
  _fields = cursor.keys()

  fields = []
  for x in _fields:
    if x not in fields:
      fields.append(x)
  
  output = []
  for result in cursor:
    row = ()
    for f in fields:
      row += (result[f],)

    output.append(row)

  fields = [(f,) for f in fields]

  cursor.close()
  context = dict(table=output, fields=fields)
  return render_template("ratinghome.html", **context)

@app.route('/keywords', methods=['GET', 'POST'])
def keywords():
  search = request.form['search']

  query = 'select distinct title, duration, language from (select title, name, duration, language, unnest(keywords) as key from (select * from artists join writes using(artistid)) as a join songs using(songid)) as b where (%s) = key'
  cursor = g.conn.execute(query, (search,))

  _fields = cursor.keys()

  fields = []
  for x in _fields:
    if x not in fields:
      fields.append(x)
  
  output = []
  for result in cursor:
    row = ()
    for f in fields:
      row += (result[f],)

    output.append(row)

  fields = [(f,) for f in fields]
  cursor.close()
  context = dict(search=str(search), table=output, fields=fields)
  return render_template("keywords.html", **context)

# Example of adding new data to the database
@app.route('/add', methods=['POST', 'GET'])
def add():
    if request.method == 'GET':
      redirect('/')
    username = request.form["userid"]
    name = request.form["name"]
    genre = request.form["genre"]
    mood = request.form["mood"]

    cursor = g.conn.execute("SELECT userid FROM users")
    usernames = []
    for result in cursor:
      usernames.append(result[0])
    cursor.close()

    if username not in usernames:
      g.conn.execute('INSERT INTO users(userid, name, favoritegenre, favoritemood) VALUES((%s), (%s), (%s), (%s))', username, name, genre, mood)

    artists = request.form['artists']

    albums = []
    artists_list = artists.split(', ')
    for artist in artists_list:
        cursor = g.conn.execute("SELECT artistid FROM artists WHERE name = (%s)", artist)
        a = []
        for result in cursor:
          a.append(result[0])
        cursor.close()
        if a:
          g.conn.execute("INSERT INTO follows(userid, artistid) VALUES((%s), (%s))", username, a[0])
          cursor = g.conn.execute("SELECT title, releasedate, awards FROM(SELECT * from album NATURAL JOIN releases NATURAL JOIN artists WHERE artistid = %s) as a", a[0])
          for result in cursor:
            albums.append(result[:])
          cursor.close()
    

    mood_dict = dict()
    mood_dict["sad"] = ["pain", "rain", "hate", "break", "wasted", "sorry", "bottle", "sad", "die", "paranoid", "rain", "tears", "teardrops", "bleeding", "wishing", "well", "scar", "alone"]
    mood_dict["happy"] = ["smile", "dance", "crazy", "money", "prosper", "angel", "happy", "happier", "money", "heavier", "heart", "light", "god", "pretty", "bright"]
    mood_dict["chill"] = ["galaxy", "levitating", "study", "life", "sky", "forest", "laugh", "heaven", "blue", "grey", "moonlight", "stars"]
    mood_dict["love"] = ["baby", "bae", "beautiful", "laugh", "love", "pretty", "touch", "lover", "closer", "sin", "babe", "honey", "wish"]

    songs = []
    for key in mood_dict[mood]:
      cursor = g.conn.execute("SELECT songid, name, title from(select songid, title from (select songid, title, unnest(keywords) as keys from songs group by songid, title) as t1 where keys=%s) as t2 join writes using(songid) join artists using(artistid)", key)
      for result in cursor:
        songs.append(result[:])

    cursor.close()
    songs = list(set([i for i in songs]))
    random_num = random.randint(0, len(songs)-1)

    context = dict(data=songs[random_num], username = username, albums = albums)
    return render_template('add.html', **context)

@app.route('/login')
def login():
    abort(401)
    this_is_never_executed()

@app.route('/rate', methods=['GET', 'POST'])
def rate():
    username = request.form["username"]
    rating = request.form["rating"]
    songid = request.form["songid"]

    cursor = g.conn.execute("SELECT userid FROM users")
    usernames = []
    for result in cursor:
      usernames.append(result[0])
    cursor.close()

    if username not in usernames:
      return render_template('fail.html')

    cursor = g.conn.execute("SELECT songid from songs")
    songs = []
    for result in cursor:
      songs.append(result[0])
    cursor.close()

    if songid not in songs:
      return render_template('fail.html')

    g.conn.execute('insert into rates(userid, songid, rating) values((%s), (%s), (%s))', username, songid, rating)

    return render_template('success.html')

if __name__ == "__main__":
  import click

  @click.command()
  @click.option('--debug', is_flag=True)
  @click.option('--threaded', is_flag=True)
  @click.argument('HOST', default='0.0.0.0')
  @click.argument('PORT', default=8111, type=int)
  def run(debug, threaded, host, port):
    """
    This function handles command line parameters.
    Run the server using:

        python server.py

    Show the help text using:

        python server.py --help

    """
    HOST, PORT = host, port
    print("running on %s:%d" % (HOST, PORT))
    app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)

  run()