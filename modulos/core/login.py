
        izquierda, centro, derecha = st.columns([1, 1.15, 1])
        with centro:
            with st.container(border=True):
                st.image("assets/NEXO.JPEG", use_container_width=False)
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
