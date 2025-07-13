import json
import boto3
import uuid
from pdfrw import PdfReader, PdfWriter, PdfName, PdfDict, PdfObject

s3 = boto3.client('s3')

# 設定
BUCKET_NAME = '2025-leagabot-1'
TEMPLATE_KEY = 'input/test.pdf'

# --- ユーティリティ関数 ---

def set_fields_readonly(pdf_in, pdf_out, fields=None):
    """
    指定したフォームフィールドを読み取り専用にします。
    fields が None の場合は全フィールドを対象とします。
    fields: List[str] or None
    """
    pdf = PdfReader(pdf_in)
    for page in pdf.pages:
        annots = page.get('/Annots')
        if not annots:
            continue
        for annot in annots:
            if annot['/Subtype'] == PdfName.Widget and annot.get('/T'):
                key = annot['/T'][1:-1]
                if fields is None or key in fields:
                     # /Ff フラグの既存値を取得（None の場合は 0）
                    existing_obj = annot.get(PdfName('Ff'))
                    flags = int(existing_obj) if existing_obj is not None else 0
                    annot.update(PdfDict(Ff=flags | 1))
    if pdf.Root.AcroForm:
        pdf.Root.AcroForm.update(
            PdfDict(NeedAppearances=PdfObject('true'))
        )
    PdfWriter().write(pdf_out, pdf)


def fill_pdf(input_pdf_path, output_pdf_path, data):
    """
    フォームフィールドに値をセットし、描画再生成フラグを立てます。
    """
    template = PdfReader(input_pdf_path)
    for page in template.pages:
        annots = page.get('/Annots')
        if not annots:
            continue
        for annot in annots:
            if annot['/Subtype'] == PdfName.Widget and annot.get('/T'):
                key = annot['/T'][1:-1]
                if key in data:
                    annot.update(PdfDict(
                        V=str(data[key]),
                        AS=str(data[key])
                    ))
    if template.Root.AcroForm:
        template.Root.AcroForm.update(
            PdfDict(NeedAppearances=PdfObject('true'))
        )
    PdfWriter().write(output_pdf_path, template)


def set_button_visibility(pdf_in, pdf_out, visibility_map):
    """
    汎用: visibility_map のキーにマッチするフォームフィールドの表示/非表示を制御します。
    visibility_map: { 'field_name': bool, ... }
    非表示にしたいものはページのAnnotsリストからも削除します。
    """
    pdf = PdfReader(pdf_in)
    for page in pdf.pages:
        annots = page.get('/Annots')
        if not annots:
            continue

        new_annots = []
        for annot in annots:
            # Widget アノテーションだけ対象
            if annot['/Subtype'] == PdfName.Widget and annot.get('/T'):
                key = annot['/T'][1:-1]
                if key in visibility_map:
                    if visibility_map[key]:
                        # 表示したいものはフラグをクリアして残す
                        annot.update(PdfDict(F=0))
                        new_annots.append(annot)
                    # 非表示にしたいものは new_annots に追加しない → ページから削除
                    continue

            # その他のアノテーションはそのまま残す
            new_annots.append(annot)

        # 更新したAnnotsリストをセット
        if new_annots:
            page.Annots = new_annots
        else:
            # 全部消えたらキー自体を削除
            page.pop('/Annots', None)

    # 描画更新用フラグも立てておく
    if pdf.Root.AcroForm:
        pdf.Root.AcroForm.update(
            PdfDict(NeedAppearances=PdfObject('true'))
        )

    PdfWriter().write(pdf_out, pdf)


# --- Lambda ハンドラー ---

def lambda_handler(event, context):
    # 1. リクエストデータ取得
    body = json.loads(event['body'])

    # 2. S3からテンプレートPDFをダウンロード
    template_path = body.get('template_path', TEMPLATE_KEY)
    input_pdf_path = '/tmp/template.pdf'
    s3.download_file(BUCKET_NAME, template_path, input_pdf_path)

    # 3. フォームにデータを流し込む
    filled_pdf_path = '/tmp/filled.pdf'
    fill_pdf(input_pdf_path, filled_pdf_path, body)

    # 4. 読み取り専用ロック
    locked_pdf_path = '/tmp/locked.pdf'
    set_fields_readonly(filled_pdf_path, locked_pdf_path)

    # 5. ボタン表示/非表示の制御
    visibility_map = body.get('visibility_map')
    visibility_pdf_path = '/tmp/visibility.pdf'
    set_button_visibility(locked_pdf_path, visibility_pdf_path, visibility_map)

    # 6. S3にアップロード
    output_key = f'output/{uuid.uuid4()}.pdf'
    s3.upload_file(visibility_pdf_path, BUCKET_NAME, output_key)

    # 7. 署名付きURL生成
    url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={'Bucket': BUCKET_NAME, 'Key': output_key},
        ExpiresIn=3600
    )

    return {
        'statusCode': 200,
        'body': json.dumps({'url': url})
    }
