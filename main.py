#from gcpsql import GCP
from typing import Optional, List
from datetime import datetime, timedelta
import random
import numpy as np
from scipy.sparse import csr_matrix, load_npz
from urllib.parse import unquote

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware

from pydantic import BaseModel
from starlette.responses import RedirectResponse
from jose import JWTError, jwt
from passlib.context import CryptContext

from cockroach import Cockroach


sample_reviews = {
    "reviews": [
        {"movie_id": 13455, "score": 69, "review": "Great fucking movie."},
        {
            "movie_id": 1366,
            "score": 88,
            "review": "It was alright. I mean i liked it, but i didn't like like it. does that make sense? It's like kind of the feeling you get when your're like sucking your roommates toes and like he likes it and you can see that so you're kind of liking it",
        },
        {
            "movie_id": 197,
            "score": 33,
            "review": "It really wasn't that great of an experience. The movie itself was alright, but the guy next to me was really stinky so...",
        },
        {"movie_id": 4951, "score": 11, "review": "Worse than a thousand 9/11s."},
    ]
}

# JWT Encoding paramaters
SECRET_KEY = "454d65c011f30bba0811c14000148859eef72a8bdeb45ff2570becc3178dccd3"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 1440

# Site parameters
CALIBRATION_COUNT = 10
MIN_COMMON = 3


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    email: Optional[str] = None


class User(BaseModel):
    email: str
    full_name: Optional[str] = None
    disabled: Optional[bool] = None


class UserInDB(User):
    hashed_password: str


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

db = Cockroach()
app = FastAPI()
# try:
#     gcp = GCP()
# except Exception as e:
#     print("ERROR: ", e)

# CORS Policy
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========
# Auth
# =========


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def get_user(email: str):
    hash = db.get_auth(email)
    if hash:
        user_dict = {"email": email, "hashed_password": hash}
        return UserInDB(**user_dict)


def authenticate_user(email: str, password: str):
    user = get_user(email)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
        token_data = TokenData(email=email)
    except JWTError:
        raise credentials_exception
    user = get_user(email=token_data.email)
    if user is None:
        raise credentials_exception
    return user


# ===========
# Algorithms
# ===========

review_mtx = load_npz("data/sparse_ratings.npz")
critic_map = np.load("data/critics.npy", allow_pickle=True)
movie_ids = np.load("data/tmdb_ids.npy", allow_pickle=True)
print("running global scope")


def get_preference_vector(user_ratings):
    vec = np.zeros((review_mtx.shape[1]))
    vec[:] = np.NaN

    for review in user_ratings:
        # TODO: handle case where id isn't present in movie_ids
        el = np.where(movie_ids == review["id"])[0]
        if el.any():
            vec[el] = review["rating"]
        else:
            print("Movie", review["id"], "doesn't exist in movie_ids")

    return vec


def closest_critic(user_prefs, num_common=MIN_COMMON):
    # Get non-zero row/col pairs in review matrix
    rows, cols = review_mtx.nonzero()

    # Get rated movies
    rated_cols = np.where(~np.isnan(user_prefs))[0]

    # Gets the index in cols, where cols has a value in rated cols
    rated_idxs = np.array(
        [
            cols_idx
            for rated_col in rated_cols
            for cols_idx in np.where(cols == rated_col)[0]
        ]
    )

    if len(rated_idxs) == 0:
        return "", -1

    # Calculate sum of square difference for each critic
    critic_deltas = {}
    for idx in rated_idxs:
        critic, movie_col = rows[idx], cols[idx]
        critic_rating = review_mtx[critic, movie_col]

        if critic not in critic_deltas:
            critic_deltas[critic] = []
        critic_deltas[critic] += [((user_prefs[movie_col] - critic_rating) ** 2, movie_col)]

    # Drop critics who have less than N reviews in common
    critic_deltas = {
        critic: deltas
        for critic, deltas in critic_deltas.items()
        if len(deltas) >= num_common
    }

    # Accumulate deltas
    total_deltas = {
        critic: sum([d[0] for d in deltas]) for critic, deltas in critic_deltas.items()
    }

    # find closest critic
    min_critic = min(total_deltas, key=total_deltas.get)

    # Build dict of movie, critic rating, user rating for best match
    match_movies = [
        {
            "movie_id": int(movie_ids[movie_col]),
            "critic_rating": float(review_mtx[min_critic, movie_col]),
            "user_rating": float(user_prefs[movie_col]),
        }
        for _, movie_col in critic_deltas[min_critic]
    ]

    return critic_map[min_critic], match_movies


def get_next(seen_movies):
    movie_picks = [
        (13455, 42),
        (1366, 69),
        (197, 85),
        (4951, 69),
        (11324, 76),
        (584, 50),
        (1824, 65),
        (65, 54),
        (245, 55),
        (2105, 61),
        (818, 44),
        (310, 57),
        (118, 51),
        (411, 61),
        (11247, 53),
        (435, 50),
        (141, 80),
        (601, 72),
        (12, 86),
        (98, 87),
        (674, 74),
        (675, 81),
        (8488, 62),
        (9806, 75),
        (24, 81),
        (254, 50),
        (4964, 83),
        (9291, 62),
        (508, 72),
        (603, 85),
        (10625, 66),
        (693, 58),
        (787, 58),
        (11036, 85),
        (161, 80),
        (285, 72),
        (22, 86),
        (114, 68),
        (565, 48),
        (9816, 59),
        (215, 59),
        (4247, 43),
        (1584, 64),
        (809, 69),
        (810, 52),
        (187, 78),
        (557, 67),
        (559, 51),
        (1895, 66),
        (122, 86),
        (544, 61),
        (597, 69),
        (8373, 57),
        (1858, 85),
        (10229, 78),
        (74, 42),
        (9522, 70),
        (12153, 55),
    ]
    unrated = [x for x in movie_picks if x[0] not in seen_movies]
    return random.choice(unrated)


#def get_unseen_critic_movies(critic_id: str):
    #


# =========
# Routes
# =========

@app.get("/")
def index():
    return RedirectResponse(url="/docs")


@app.get("/calibrated")
async def is_calibrated(current_user: User = Depends(get_current_user)):
    ratings = db.pull_ratings(current_user.email) 
    if ratings is None: return False
    ratings = [True for item in ratings if item['rating'] >= 0]
    return {"calibrated": len(ratings) >= CALIBRATION_COUNT}


@app.get("/critic/{critic_id}")
async def get_critic(critic_id: str):  # TODO: Return all critic reviews
    # list of: critic name, tmdb id, score, contents
    critic_id = unquote(critic_id)
    return '' #gcp.get_critic("Roger Moore")


@app.post("/ratings", status_code=status.HTTP_200_OK)
async def get_ratings(current_user: User = Depends(get_current_user)):
    return db.pull_ratings(current_user.email)


@app.post("/rating/", status_code=status.HTTP_200_OK)
async def rate_movie(
    movie_id: int, rating: float, current_user: User = Depends(get_current_user)
):
    db.send_rating(current_user.email, movie_id, rating)
    return {}


@app.post("/clear_ratings", status_code=status.HTTP_200_OK)
async def clear_ratings(current_user: User = Depends(get_current_user)):
    db.del_ratings(current_user.email)
    return {}
    # TODO raise http expection on error


@app.get("/rec/next")
async def get_next_rec(current_user: User = Depends(get_current_user)):
    ratings = db.pull_ratings(current_user.email)
    if ratings is None:
        next_id, avg_rating = get_next()
        return {"movie_id": next_id, "avg_rating": avg_rating}
    
    seen_ids = [item['id'] for item in ratings]
    next_id, avg_rating = get_next(seen_ids)
    return {"movie_id": next_id, "avg_rating": avg_rating}


@app.get("/rec/critic")
async def get_critic_rec(current_user: User = Depends(get_current_user)):
    user_prefs = db.pull_ratings(current_user.email)  # pull user ratings
    user_prefs = [item for item in user_prefs if item['rating'] >= 0]
    prev_vec = get_preference_vector(user_prefs)
    critic, matches = closest_critic(prev_vec)
    return {"critic_id": critic, "matches": list(matches)}


@app.post("/login", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.email}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/register", response_model=Token)
async def register(form_data: OAuth2PasswordRequestForm = Depends()):
    succ = db.add_account(form_data.username, get_password_hash(form_data.password))
    if not succ:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account could not be created",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": form_data.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}
