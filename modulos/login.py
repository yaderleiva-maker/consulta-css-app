import streamlit as st
from streamlit_oauth import OAuth2Component

CLIENT_ID = st.secrets["google"]["client_id"]
CLIENT_SECRET = st.secrets["google"]["client_secret"]

AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"

oauth2 = OAuth2Component(
    CLIENT_ID,
    CLIENT_SECRET,
    AUTHORIZE_URL,
    TOKEN_URL
)

result = oauth2.authorize_button(
    name="Ingresar con Google 🔐",
    icon="https://www.google.com/favicon.ico",
    redirect_uri="https://consulta-css-app-fq8jetxy8yzjd3hzuwmbwj.streamlit.app",
    scope="openid email profile",
    key="google"
)

if result:
    id_token = result["token"]["id_token"]

    import jwt
    user_info = jwt.decode(id_token, options={"verify_signature": False})

    email = user_info["email"]

    st.session_state.login_ok = True
    st.session_state.usuario = email

    st.success(f"Bienvenido {email} 🎉")
    st.rerun()
