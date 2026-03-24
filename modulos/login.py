import streamlit as st

# Usuarios (por ahora simple)
usuarios = {
    "yader@gmail.com": "1234",
    "supervisor@gmail.com": "abcd"
}

def login():

    if "login_ok" not in st.session_state:
        st.session_state.login_ok = False

    if "usuario" not in st.session_state:
        st.session_state.usuario = None

    if not st.session_state.login_ok:

        st.title("🔐 Acceso a la plataforma")

        usuario = st.text_input("Correo")
        password = st.text_input("Contraseña", type="password")

        if st.button("Ingresar"):
            if usuario in usuarios and usuarios[usuario] == password:
                st.session_state.login_ok = True
                st.session_state.usuario = usuario
                st.success("Acceso concedido ✅")
                st.rerun()
                # En login.py cuando hace login correcto
                st.session_state.usuario = usuario
            else:
                st.error("Credenciales incorrectas ❌")

        st.stop()

def logout():
    if st.button("Cerrar sesión"):
        st.session_state.login_ok = False
        st.session_state.usuario = None
        st.rerun()
