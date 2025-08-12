---
title: "Phần 4: Dọn dẹp tài nguyên"
weight: 40
chapter: False
---

# Phần 4: Dọn dẹp tài nguyên - Quản lý chi phí

Sau khi hoàn thành workshop, việc dọn dẹp tất cả tài nguyên là rất quan trọng để tránh các khoản phí không mong muốn. Phần này cung cấp quy trình dọn dẹp toàn diện cho tất cả tài nguyên được tạo trong workshop.

##  Mục tiêu dọn dẹp

- **Terminate EMR clusters** và các tài nguyên liên quan
- **Xóa CloudWatch** alarms, dashboards, và custom metrics
- **Xóa SNS topics** và subscriptions
- **Dọn dẹp Lambda functions** và EventBridge rules
- **Xóa S3 buckets** và logs (nếu có tạo)
- **Xác minh dọn dẹp hoàn tất** để tránh phí

##  Cảnh báo quan trọng

{{% notice warning %}}
**QUAN TRỌNG**: Việc xóa tài nguyên không thể hoàn tác. Đảm bảo bạn đã sao lưu dữ liệu quan trọng trước khi tiến hành.
{{% /notice %}}

{{% notice info %}}
**Tác động chi phí**: Để tài nguyên chạy có thể tốn $50-200+ mỗi ngày cho một EMR cluster.
{{% /notice %}}

[Nội dung tiếng Việt tương tự như bản tiếng Anh...]

##  Kết luận

**Chúc mừng!** Bạn đã hoàn thành việc dọn dẹp tài nguyên một cách an toàn.

###  Checklist cuối cùng:
- [ ] Tất cả EMR clusters đã terminate
- [ ] CloudWatch alarms và dashboards đã xóa
- [ ] SNS topics đã xóa
- [ ] Lambda functions đã xóa
- [ ] S3 buckets đã dọn dẹp
- [ ] Kiểm tra AWS Console
