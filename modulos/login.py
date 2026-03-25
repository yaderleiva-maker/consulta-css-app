import streamlit as st
from streamlit_oauth import OAuth2Component
import jwt

# Config OAuth
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

# Estado inicial
if "login_ok" not in st.session_state:
    st.session_state.login_ok = False

if not st.session_state.login_ok:

    st.title("🔐 Acceso con Google")

    result = oauth2.authorize_button(
        name="Ingresar con Google",
        icon="https://www.google.com/favicon.ico",
        redirect_uri="https://consulta-css-app-fq8jetxy8yzjd3hzuwmbwj.streamlit.app",
        scope="openid email profile",
        key="google"
    )

    if result:
        token = result["token"]["id_token"]

        user_info = jwt.decode(token, options={"verify_signature": False})

        email = user_info["email"]

        # 🔒 CONTROL DE ACCESO
        usuarios_permitidos = [
            "yader@gmail.com",
            "supervisor@gmail.com"
        ]

        if email not in usuarios_permitidos:
            st.error("❌ No tienes acceso")
            st.stop()

        # Guardar sesión
        st.session_state.login_ok = True
        st.session_state.usuario = email

        st.success(f"Bienvenido {email} 🎉")
        st.rerun()

    st.stop()
