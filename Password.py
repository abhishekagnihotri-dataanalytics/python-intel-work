from cryptography.fernet import Fernet
from Project_params import params


class Account:
    def __init__(self, username, password):
        self._username = username
        self._password = password

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    def set_password(self, password):
        self._password = password


class Application:
    def __init__(self, client_id, client_secret):
        self._client_id = client_id
        self._client_secret = client_secret

    @property
    def client_id(self):
        return self._client_id

    @property
    def client_secret(self):
        return self._client_secret

    def set_client_secret(self, client_secret):
        self._client_secret = client_secret


def retrieve_secret(file_path: str) -> str:
    """Function to read the encryption key from file.

            Args:
                file_path: [str] Path to the file containing the encryption key.

            Returns:
                [str] Encryption key as plain text.
            """
    try:
        with open(file_path, 'rb') as encryption_key:
            key = encryption_key.read()
    except FileNotFoundError:
        print('Unable to read key for password decryption.')
        key = input("Please enter the key manually: ")
    except PermissionError:
        print('User does not have access to Operational Analytics Password Management (PAM)\nPlease apply for the AGS: PAM AAM Safe Owner for GSM-BI PR')
        key = input("Please enter the key manually: ")

    return key


def encrypt_password(password: str) -> bytes:
    """Function to encrypt a string using Operational Analytics secret key.

        Args:
            password: [str] Password to be encrypted.

        Returns:
            [bytes] Encrypted password.
        """
    return __f__.encrypt(password.encode('utf-8'))


def decrypt_password(binary_password: bytes) -> str:
    """Function to decrypt a string using Operational Analytics secret key.

        Args:
            binary_password: [bytes] Encrypted password.

        Returns:
            [str] Password as plain text.
        """
    return __f__.decrypt(binary_password).decode('utf-8')


accounts = {
    "GSM Support": Account(username="grp_gsm_ssbi_support@intel.com", password=b'gAAAAABjtFWOXxShHKW7HUe0RshGYUg-YxYTLv1blBqz_iog0UBwTyjKhsD5nJ72oeHUDpovUvGYcywKnyCvzebjr68Ws85Q1J-iPR3JBtUOMWeH4Zdt7Fg='),
    "GSM Ariba": Account(username="gsmariba@intel.com", password=b'gAAAAABjtFLe-v3xWvqbMtc6axwkJ2ScAj6L87GrJN9LoQ6Mfgj6X4C2RiOLKN180CG25efE_N2NsukGyNPH3JHXdtiP-eAStVInjK39GCpL6Gho79Ae9js='),
    "TMEBI Admin": Account(username="tmebiadm@intel.com", password=b'gAAAAABiGB0oKKYpWXt4fx5xZXGZZTLj_KX5pm1lEyqnpEjtBSeIiii3o5HRry_ElgyaO34_NYQEWDC5u10Gybdr_u6HZx_AdQ=='),
    "Teradata": Account(username="APPL_GSM_BI_01", password=b'gAAAAABiGB0ogAh_giI7O8WRwx2TBXMO0Amqb5VCTnODi5yqBTOIPmGlw7_avURXbhVS1cvJdNZSgsvYKqyhQkmryYR4GcRlsA=='),
    "HANA": Account(username="SYSH_SCES_OPSRPT", password=b'gAAAAABiGB0o3kOky6MVa-R5AnO6tt5mxQJZx6lbq0WAYSrKLXqp9N33fymiDWBOVcVek0LPhVAog_AhIci5uVoFg--BXNuWZQ=='),
    "Salesforce": Account(username="icappublicwebsite@intel.com.icapprod", password=b'gAAAAABiIArIBPxiE1SkyguKxMggiR-f04LaM67FvC3Xb6asvDqEeg6dcVqzJTn7GO-IBnyoyt7O5fhjZBNXR4nDBffj2o2XmOEO6fm0lezR7K1O3a-nnNI='),
    "Smartsheet": Account(username="sys_SCData@intel.com", password=b'gAAAAABiu2rNyuoBz4AAy76JgHrVtsGsrIIh4pktvHG4iQYKDICDLVh9aowN-InYDahWwRaQzvHCKP8Fq9peoBU5gPdBPlVoIooMr6QXP5W262F7-Ln7zvAqWJgq_y4-DlVcPk50THvj'),
    "Apigee": Application(client_id="aa58ffff-eb6c-4b7f-a9fc-b63b98ca29ca", client_secret=b'gAAAAABiPgYpoux_FK9oSm5RL47VWUWBV-DRnatWAqZ4-wvxoH1bQb1n6MPtQPRcVi8GCTKRpIGmnAW-l7oimQEJaDEF_EdxSpg25g1aCqn6eqrcpATIoYMwMVGRNX2qYQWKJOGE7MS8'),
    "SharePoint": Application(client_id="57ac92cf-106d-45cc-8749-afb8ad67b4b2", client_secret=b'gAAAAABjgkp0M0daAfNnA9iCXgJwafpIM-HR4g83d_TRfERJzvXRYAvu2xNWMg6gbyyyrxRfNPeyWG8BjXKo_VNm5_TiN19oNdnwaJxtwCcciTroe-8pdCSDWiIvc4jrGgKfhT3OiVak')
}

__f__ = Fernet(retrieve_secret(params['PASS_KEY']))

# Decrypt the binary passwords and client_secrets for use
for name in accounts.keys():
    if isinstance(accounts[name], Account):
        temp = decrypt_password(accounts[name].password)
        accounts[name].set_password(temp)
    else:
        temp = decrypt_password(accounts[name].client_secret)
        accounts[name].set_client_secret(temp)
    # print(temp)
