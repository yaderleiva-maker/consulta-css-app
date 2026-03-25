import streamlit as st
import jwt
from streamlit_oauth import OAuth2Component

# -----------------------
# CONFIGURACIÓN OAUTH
# -----------------------

CLIENT_ID = st.secrets["google"]["client_id"]
CLIENT_SECRET = st.secrets["google"]["client_secret"]

AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"

# 👇 IMPORTANTE: DEFINIDO AQUÍ ARRIBA
REDIRECT_URI = "https://consulta-css-app-fq8jetxy8yzjd3hzuwmbwj.streamlit.app"

oauth2 = OAuth2Component(
    CLIENT_ID,
    CLIENT_SECRET,
    AUTHORIZE_URL,
    TOKEN_URL,
)

# -----------------------
# LOGIN
# -----------------------

def login():

    if "login_ok" not in st.session_state:
        st.session_state.login_ok = False

    if not st.session_state.login_ok:
        st.title("🔐 Acceso con Google")

        result = oauth2.authorize_button(
            name="Ingresar con Google",
            redirect_uri=REDIRECT_URI,
            scope="openid email profile",
            key="google_login"
        )

        if result:
            try:
             

        token = result.get("token", {})
        id_token = token.get("id_token")

        if id_token:
            decoded = jwt.decode(id_token, options={"verify_signature": False})
            email = decoded.get("email")
        else:
            email = None


                if not email:
                    st.error("No se pudo obtener el correo ❌")
                    st.write(result)  # 👈 DEBUG TEMPORAL
                    st.stop()

                usuarios_permitidos = [
                    "yader@gmail.com",
                    "supervisor@gmail.com",
                    "contenalfa@gmail.com"
                ]

                if email in usuarios_permitidos:
                    st.session_state.login_ok = True
                    st.session_state.usuario = email
                    st.success(f"Bienvenido {email} ✅")
                    st.rerun()
                else:
                    st.error("No tienes acceso ❌")

            except Exception as e:
                st.error(f"Error en login: {e}")

        st.stop()


# -----------------------
# LOGOUT
# -----------------------

def logout():
    if st.session_state.get("login_ok"):
        if st.button("Cerrar sesión"):
            st.session_state.clear()
            st.rerun()
