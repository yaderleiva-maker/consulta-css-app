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

REDIRECT_URI = "https://consulta-css-app-fq8jetxy8yzjd3hzuwmbwj.streamlit.app"

oauth2 = OAuth2Component(
    CLIENT_ID,
    CLIENT_SECRET,
    AUTHORIZE_URL,
    TOKEN_URL,
)


def _estilos_login():
    """Estilos exclusivos para la vista de autenticación."""
    st.markdown(
        """
        <style>
            [data-testid="stAppViewContainer"] {
                background: #f7f8fa;
            }

            [data-testid="stHeader"] {
                background: transparent;
            }

            [data-testid="stMainBlockContainer"] {
                max-width: 100%;
                padding: 2rem 1.25rem 3rem;
            }

            .nexo-login-spacer {
                height: clamp(1rem, 9vh, 6rem);
            }

            .nexo-brand {
                text-align: center;
                margin: 0.75rem 0 1.75rem;
            }

            .nexo-brand h1 {
                color: #172033;
                font-family: Inter, ui-sans-serif, system-ui, sans-serif;
                font-size: clamp(1.65rem, 3vw, 2rem);
                font-weight: 650;
                letter-spacing: -0.04em;
                line-height: 1.15;
                margin: 0;
            }

            .nexo-brand p {
                color: #697386;
                font-family: Inter, ui-sans-serif, system-ui, sans-serif;
                font-size: 0.98rem;
                margin: 0.6rem 0 0;
            }

            /* Tarjeta creada con st.container(border=True). */
            [data-testid="stVerticalBlockBorderWrapper"] {
                background: #ffffff;
                border: 1px solid #e6e9ef;
                border-radius: 18px;
                box-shadow: 0 12px 32px rgba(23, 32, 51, 0.08);
                padding: clamp(1.5rem, 4vw, 2.5rem) !important;
            }

            [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stImage"] img {
                width: min(132px, 42vw);
                margin: 0 auto;
            }

            [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stImage"] {
                display: flex;
                justify-content: center;
            }

            /* El componente OAuth conserva su misma llamada; solo cambia el acabado. */
            [data-testid="stVerticalBlockBorderWrapper"] button {
                min-height: 46px;
                border-radius: 10px !important;
                border: 1px solid #d9dee8 !important;
                box-shadow: none !important;
                font-weight: 600 !important;
            }

            [data-testid="stVerticalBlockBorderWrapper"] button:hover {
                border-color: #aeb8c9 !important;
                background: #f8fafc !important;
            }

            @media (max-width: 640px) {
                [data-testid="stMainBlockContainer"] {
                    padding: 1rem 1rem 2rem;
                }

                [data-testid="stVerticalBlockBorderWrapper"] {
                    border-radius: 14px;
                }
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


# -----------------------
# LOGIN
# -----------------------

def login():

    if "login_ok" not in st.session_state:
        st.session_state.login_ok = False

    if not st.session_state.login_ok:
        _estilos_login()
        st.markdown('<div class="nexo-login-spacer"></div>', unsafe_allow_html=True)

        izquierda, centro, derecha = st.columns([1, 1.15, 1])
        with centro:
            with st.container(border=True):
                st.image("assets/NEXO.jpeg", use_container_width=False)
                st.markdown(
                    """
                    <div class="nexo-brand">
                        <h1>Nexo Notebook</h1>
                        <p>Tu espacio de trabajo inteligente</p>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                # Lógica OAuth original: no modificar.
                result = oauth2.authorize_button(
                    name="Ingresar con Google",
                    redirect_uri=REDIRECT_URI,
                    scope="openid email profile",
                    key="google_login"
                )

                if result:
                    try:
                        # 🔥 EXTRAER TOKEN
                        token = result.get("token", {})
                        id_token = token.get("id_token")

                        if id_token:
                            decoded = jwt.decode(id_token, options={"verify_signature": False})
                            email = decoded.get("email")
                        else:
                            email = None

                        # VALIDACIÓN
                        if not email:
                            st.error("No se pudo obtener el correo ❌")
                            st.write(result)
                            st.stop()

                        # 🔒 CONTROL DE ACCESO
                        usuarios_permitidos = [
                            "yaderleiva@gmail.com",
                            "supervisor@gmail.com",
                            "contenalfa@gmail.com",
                            "arismaytte@gmail.com",
                            "sgonzalez.hex@gmail.com",
                            "yesturainhexagon@gmail.com",
                            "mariachacon@hopsa.com",
                            "gabrielamorales@hopsa.com",
                            "contenbeta@gmail.com",
                            "aburgos2630@gmail.com",
                            "condadodelreyfarmazone@gmail.com",
                            "tmksolutionspty@gmail.com",
                            "nalvaradohexagon@gmail.com",
                            "hexagonclaudia@gmail.com",
                            "clautotini1224@gmail.com"
                        ]

                        if email in usuarios_permitidos:
                            st.session_state.login_ok = True
                            st.session_state.usuario = email
                            st.success(f"Bienvenido {email} ✅")
                            st.rerun()
                        else:
                            st.error("No tienes acceso ❌")
                            st.markdown(
                                """
                                <script>
                                setTimeout(function(){
                                window.location.reload();
                                }, 2000);
                                </script>
                                """,
                                unsafe_allow_html=True
                            )

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
            st.markdown(
                """
                <script>
                    window.location.reload();
                </script>
                """,
                unsafe_allow_html=True
            )
